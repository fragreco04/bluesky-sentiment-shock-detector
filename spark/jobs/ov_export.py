"""
Esporta il modello di sentiment in formato OpenVINO (IR) e crea le varianti int8 con NNCF.

Ogni variante finisce in SENTIMENT_OV_DIR/<variante>/model.xml (+ model.bin e info.json):
  fp32       conversione diretta da PyTorch: stesso calcolo, motore OpenVINO
  int8       quantizzazione statica per transformer con SmoothQuant (alpha 0.95, default NNCF)
  int8-sq75  SmoothQuant con alpha 0.75
  int8-sq50  SmoothQuant con alpha 0.50
  int8-nosq  int8 senza SmoothQuant (per misurare quanto aiuta)
  int8-acc   int8 con controllo dell'accuratezza: i layer piu' dannosi tornano fp32
             finche' l'accordo con il fp32 non perde piu' di --max-drop (lenta)

Le varianti int8 usano post reali per calcolare in anticipo le scale (calibrazione).
Si usano i post DOPO i primi --skip del campione: i primi restano al benchmark,
cosi' le varianti non vengono valutate sugli stessi post su cui sono state tarate.

Uso (dentro l'immagine Spark, con Q1 fermo: servono 2-3 GB di RAM):
  python3 ov_export.py sample.jsonl
  python3 ov_export.py sample.jsonl --variants int8-acc --max-drop 0.02
"""
import argparse
import datetime
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sentiment_model  # noqa: E402  (preprocess e nomi delle varianti: stesso codice della pipeline)

INPUT_NAMES = ("input_ids", "attention_mask")

# Forza di SmoothQuant per variante: quanto dei valori estremi delle attivazioni viene
# spostato nei pesi. -1 = disattivato. Piu' alto: attivazioni piu' facili da quantizzare,
# pesi piu' difficili. Il valore migliore dipende dal modello: si misura.
SQ_ALPHA = {"int8": 0.95, "int8-sq75": 0.75, "int8-sq50": 0.50, "int8-nosq": -1}


def read_texts(path):
    texts = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                t = json.loads(line).get("text")
            except json.JSONDecodeError:
                continue
            if isinstance(t, str) and t.strip():
                texts.append(sentiment_model.preprocess(t))
    return texts


def save(model, out_dir, variant, info):
    import openvino as ov

    target = os.path.join(out_dir, variant)
    tmp = target + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    # compress_to_fp16=False: le costanti rimaste in virgola mobile (embedding, LayerNorm,
    # bias) restano fp32. Con il default verrebbero arrotondate a fp16, cambiando i risultati.
    ov.save_model(model, os.path.join(tmp, "model.xml"), compress_to_fp16=False)
    with open(os.path.join(tmp, "info.json"), "w") as f:
        json.dump(info, f, indent=2)
    # Sostituzione in un colpo solo: un Q1 che legge la variante non vede mai file a meta'
    shutil.rmtree(target, ignore_errors=True)
    os.replace(tmp, target)
    print(f"  salvato {target}/model.xml")


def export_fp32(model_path):
    import openvino as ov
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer
    from transformers.utils import logging as hf_logging

    hf_logging.set_verbosity_error()
    tok = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    hf = AutoModelForSequenceClassification.from_pretrained(model_path, local_files_only=True).eval()

    class Logits(torch.nn.Module):
        # Ingressi espliciti e uscita = solo i logit: grafo semplice da convertire
        def __init__(self, m):
            super().__init__()
            self.m = m

        def forward(self, input_ids, attention_mask):
            return self.m(input_ids=input_ids, attention_mask=attention_mask).logits

    example = tok(["a short example", "a slightly longer example sentence"], padding=True, return_tensors="pt")
    with torch.no_grad():
        ov_model = ov.convert_model(Logits(hf), example_input=(example["input_ids"], example["attention_mask"]))

    for port, name in zip(ov_model.inputs, INPUT_NAMES):
        port.get_tensor().set_names({name})
    ov_model.outputs[0].get_tensor().set_names({"logits"})
    # Batch e lunghezza variabili: ogni micro-batch ha forme diverse
    if any(port.get_partial_shape().is_static for port in ov_model.inputs):
        ov_model.reshape({name: ov.PartialShape([-1, -1]) for name in INPUT_NAMES})
    return ov_model, hf, tok


def check_fp32(ov_model, hf, tok, texts):
    # Controllo di coerenza: OpenVINO fp32 deve dare (quasi) gli stessi logit di PyTorch
    import numpy as np
    import openvino as ov
    import torch

    enc = tok(texts[:64], padding=True, truncation=True, max_length=256, return_tensors="np")
    inputs = {k: enc[k] for k in INPUT_NAMES}
    got = ov.Core().compile_model(ov_model, "CPU")(inputs)[0]
    with torch.no_grad():
        ref = hf(**{k: torch.from_numpy(v) for k, v in inputs.items()}).logits.numpy()
    diff = float(np.abs(got - ref).max())
    same = float((got.argmax(-1) == ref.argmax(-1)).mean())
    print(f"  controllo su 64 post: |diff logit| max {diff:.2e}, etichette uguali {100 * same:.1f}%")
    if same < 0.99:
        sys.exit("  OpenVINO fp32 non coincide con PyTorch: conversione da verificare, mi fermo")


def tokenize_items(tok, texts, batch, max_length):
    # Lista di dizionari pronti per OpenVINO, un batch di testi per elemento
    items = []
    for i in range(0, len(texts), batch):
        enc = tok(texts[i:i + batch], padding=True, truncation=True, max_length=max_length, return_tensors="np")
        items.append({k: enc[k] for k in INPUT_NAMES})
    return items


def quantize(fp32_model, calib_items, sq_alpha: float):
    import nncf

    params = nncf.AdvancedQuantizationParameters(
        # matmul=-1 disattiva SmoothQuant; 0.95 e' il valore che NNCF usa per i transformer
        smooth_quant_alphas=nncf.AdvancedSmoothQuantParameters(matmul=sq_alpha)
    )
    return nncf.quantize(
        fp32_model,
        nncf.Dataset(calib_items),
        preset=nncf.QuantizationPreset.MIXED,     # pesi simmetrici, attivazioni asimmetriche
        model_type=nncf.ModelType.TRANSFORMER,    # lascia in fp32 softmax, LayerNorm e simili
        subset_size=len(calib_items),
        advanced_parameters=params,
    )


def quantize_acc(fp32_model, calib_items, valid_texts, tok, max_length, max_drop):
    import nncf
    import numpy as np
    import openvino as ov

    # Etichette di riferimento: quelle del fp32 sui post di validazione
    ref_model = ov.Core().compile_model(fp32_model, "CPU")
    valid = []
    for item in tokenize_items(tok, valid_texts, 32, max_length):
        valid.append({"inputs": item, "ref": ref_model(item)[0].argmax(-1)})

    def agreement(compiled, data):
        # Metrica: quota di etichette uguali al fp32 (il fp32 vale 1.0 per definizione)
        same = total = 0
        for item in data:
            pred = np.asarray(compiled(item["inputs"])[0]).argmax(-1)
            same += int((pred == item["ref"]).sum())
            total += len(pred)
        return same / total

    return nncf.quantize_with_accuracy_control(
        fp32_model,
        nncf.Dataset(calib_items),
        nncf.Dataset(valid, lambda item: item["inputs"]),
        validation_fn=agreement,
        max_drop=max_drop,
        drop_type=nncf.DropType.ABSOLUTE,
        preset=nncf.QuantizationPreset.MIXED,
        model_type=nncf.ModelType.TRANSFORMER,
        subset_size=len(calib_items),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample", nargs="?", help="JSONL di posts.raw (serve per le varianti int8)")
    ap.add_argument("--variants", default="fp32,int8,int8-nosq")
    ap.add_argument("--skip", type=int, default=2000, help="post iniziali riservati al benchmark (default 2000)")
    ap.add_argument("--calib", type=int, default=300, help="post per la calibrazione (default 300)")
    ap.add_argument("--valid", type=int, default=600, help="post di validazione per int8-acc (default 600)")
    ap.add_argument("--max-drop", type=float, default=0.02, help="int8-acc: perdita massima di accordo (0.02 = 2 punti)")
    ap.add_argument("--max-length", type=int, default=int(os.getenv("INFER_MAX_LENGTH", "256")))
    ap.add_argument("--model-path", default=os.getenv("SENTIMENT_MODEL_PATH"))
    ap.add_argument("--out", default=os.getenv("SENTIMENT_OV_DIR"))
    args = ap.parse_args()

    if not args.model_path or not args.out:
        sys.exit("Servono SENTIMENT_MODEL_PATH e SENTIMENT_OV_DIR (o --model-path e --out)")
    variants = [v.strip().lower() for v in args.variants.split(",") if v.strip()]
    for v in variants:
        if v not in sentiment_model.OV_VARIANTS:
            sys.exit(f"Variante sconosciuta: {v}. Valori: {', '.join(sentiment_model.OV_VARIANTS)}")

    calib_texts, valid_texts = [], []
    if any(v != "fp32" for v in variants):
        if not args.sample:
            sys.exit("Le varianti int8 richiedono il campione di post (primo argomento)")
        extra = read_texts(args.sample)[args.skip:]
        calib_texts = extra[: args.calib]
        valid_texts = extra[args.calib: args.calib + args.valid]
        if len(calib_texts) < 100:
            sys.exit(f"Solo {len(calib_texts)} post di calibrazione dopo i primi {args.skip}: ne servono almeno 100")
        if "int8-acc" in variants and len(valid_texts) < 200:
            sys.exit(f"Solo {len(valid_texts)} post di validazione per int8-acc: ne servono almeno 200")
        print(f"Calibrazione: {len(calib_texts)} post, validazione: {len(valid_texts)} post "
              f"(dopo i primi {args.skip}, riservati al benchmark)")

    import openvino as ov

    info = {
        "source_model": args.model_path,
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "openvino": ov.get_version(),
        "calibration_posts": len(calib_texts),
    }
    try:
        import nncf
        info["nncf"] = nncf.__version__
    except ImportError:
        pass

    t0 = time.perf_counter()
    print("Conversione PyTorch -> OpenVINO fp32 ...")
    fp32, hf, tok = export_fp32(args.model_path)
    check_fp32(fp32, hf, tok, calib_texts or ["what a great day", "this is awful", "the meeting is at 5"])
    del hf
    if "fp32" in variants:
        save(fp32, args.out, "fp32", {**info, "variant": "fp32"})

    # Un post per elemento: le statistiche di calibrazione si raccolgono post per post
    calib_items = tokenize_items(tok, calib_texts, 1, args.max_length) if calib_texts else []

    for variant in variants:
        if variant == "fp32":
            continue
        t = time.perf_counter()
        print(f"Quantizzazione {variant} ...")
        # clone(): ogni variante parte dal fp32 originale, mai da una gia' quantizzata
        if variant in SQ_ALPHA:
            q = quantize(fp32.clone(), calib_items, SQ_ALPHA[variant])
        else:
            q = quantize_acc(fp32.clone(), calib_items, valid_texts, tok, args.max_length, args.max_drop)
        save(q, args.out, variant, {**info, "variant": variant, "smooth_quant_alpha": SQ_ALPHA.get(variant),
                                    "max_drop": args.max_drop if variant == "int8-acc" else None})
        print(f"  {variant}: {time.perf_counter() - t:.0f} s")

    print(f"Fatto in {time.perf_counter() - t0:.0f} s")


if __name__ == "__main__":
    main()