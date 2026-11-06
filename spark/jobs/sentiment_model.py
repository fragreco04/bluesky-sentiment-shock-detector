"""
Caricamento e inferenza del modello di sentiment, lato executor.

Perche' un modulo separato e non codice dentro il job:
le variabili globali di un modulo *importato* restano vive per tutta la vita
del processo Python worker, che Spark riusa tra un task e l'altro
(spark.python.worker.reuse=true, il default). Cosi' il modello viene caricato
una volta per worker e non a ogni micro-batch. Le globali definite nello script
principale invece vengono ri-serializzate con la UDF a ogni task, quindi una
cache messa li' si svuoterebbe ogni volta.

Thread: ogni task che esegue la UDF e' un processo Python con la sua copia del modello.
I task in parallelo sono spark.cores / spark.task.cpus, e ognuno usa num_threads thread
torch (= spark.task.cpus). La somma non deve superare i core fisici: i thread OpenMP di
torch si aspettano a vicenda, e se uno viene sospeso dal sistema rallentano tutti.

Motori di inferenza (parametro `engine`, dal .env: INFER_ENGINE):
  "torch"                 PyTorch fp32: il riferimento
  "torch:<variante>"      PyTorch con quantizzazione dinamica int8 (QUANT_MODES)
  "ov:<variante>"         OpenVINO su CPU, con il modello esportato da ov_export.py (OV_VARIANTS)
  "ov:<variante>@GPU"     OpenVINO sulla GPU integrata (serve /dev/dxg nel container)
Il nome del motore finisce nel campo `model` dei post: motori diversi danno valori
(anche di poco) diversi, e Q2/Q3 non li mescolano.
"""
from __future__ import annotations

import os
import re
import threading
from typing import List, Sequence, Tuple

_lock = threading.Lock()
_cache: dict = {}

EXPECTED_LABELS = {"negative", "neutral", "positive"}

# Varianti di quantizzazione dinamica int8 dei layer lineari.
#   nome -> (pesi con una scala per canale?, layer da lasciare in fp32)
# - per tensore: una sola scala per tutta la matrice dei pesi (il default di torch, il piu' grezzo);
# - per canale: una scala per ogni riga della matrice, segue meglio pesi di ampiezza diversa;
# - nohead: la testa di classificazione resta fp32 (decide direttamente l'etichetta, costa poco);
# - noffn: restano fp32 anche i layer FFN di uscita (3072 -> 768), che ricevono le attivazioni
#   con i valori estremi piu' marcati di RoBERTa: li' la quantizzazione perde di piu'.
# Il nome della variante finisce nel campo `model` (es. "...latest+int8pc"):
# varianti diverse danno valori diversi e Q2/Q3 le tengono separate.
QUANT_MODES = {
    "int8":          (False, ()),
    "int8pc":        (True,  ()),
    "int8pc-nohead": (True,  (r"^classifier\.",)),
    "int8pc-noffn":  (True,  (r"^classifier\.", r"\.layer\.\d+\.output\.dense$")),
}

# Varianti OpenVINO: una cartella ciascuna in SENTIMENT_OV_DIR, create da ov_export.py
#   fp32       stesso calcolo di PyTorch, con il motore ottimizzato di OpenVINO
#   int8       quantizzazione statica NNCF per transformer, con SmoothQuant alpha 0.95
#              (sposta i valori estremi delle attivazioni nei pesi, dove l'int8 li regge meglio)
#   int8-sq75  SmoothQuant piu' leggero (alpha 0.75): meno valori spostati nei pesi
#   int8-sq50  SmoothQuant ancora piu' leggero (alpha 0.50)
#   int8-nosq  come int8 ma senza SmoothQuant: serve a misurare quanto aiuta
#   int8-acc   NNCF riporta in fp32 i layer che peggiorano di piu' l'accordo col fp32,
#              finche' la perdita resta entro la soglia data a ov_export.py
OV_VARIANTS = ("fp32", "int8", "int8-sq75", "int8-sq50", "int8-nosq", "int8-acc")
OV_DEVICES = ("CPU", "GPU")
# Sulla GPU le forme variabili costano: ogni nuova forma va compilata. Lunghezze arrotondate
# a pochi valori fissi e batch sempre pieno -> poche forme, compilate una volta e messe in cache.
GPU_SEQ_BUCKETS = (16, 32, 48, 64, 96, 128, 192, 256, 384, 512)

# Gestione delle menzioni: 'Ciao @ciccio' -> '@ciccio'
_MENTION = re.compile(r"(?<![\w@])@\w(?:[\w.-]*\w)?")

# Gestione degli URL: 'Ciao https://ciccio.it/c' -> 'https://ciccio.it/c'
_TAIL = r"(?:[^\s<>\"']*[^\s<>\"'.,;:!?)\]}])?(?:\.\.\.|…)?"
_URL_SCHEME = re.compile(r"(?i)\bhttps?://" + _TAIL)

# Per non scambiare "fine.Then" o "e.g." per un dominio.
# un dominio nudo viene riconosciuto solo se:
#   1) ha un percorso (dominio/qualcosa), oppure
#   2) inizia con www., oppure
#   3) ha un TLD comune.
_LABEL = r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
_COMMON_TLDS = (
    "com|net|org|io|co|app|social|dev|me|tv|ly|gg|xyz|info|news|blog|edu|gov|"
    "us|uk|it|de|fr|es|eu|ca|au|jp|br|nl|be|ch|in|ai|fm|to|link|page|site|online|tech"
)
_URL_BARE = re.compile(
    r"(?i)(?<![\w@./-])(?:"
    r"(?:www\.)?(?:" + _LABEL + r"\.)+[a-z]{2,24}/" + _TAIL
    + r"|www\.(?:" + _LABEL + r"\.)+[a-z]{2,24}\b"
    + r"|(?:" + _LABEL + r"\.)+(?:" + _COMMON_TLDS + r")\b(?!\.\w)"
    + r")"
)


# Restituisce il testo normalizzato
def preprocess(text: str) -> str:
    # Menzioni -> "@user", link (con o senza schema, anche accorciati) -> "http".

    # Esempi:
    # "Grande post di @nome.bsky.social, leggete!" -> "Grande post di @user, leggete!"
    #  "Read this: github.com/apache/spark/pul... ok" -> "Read this: http ok"
    #  "mail me at mario@example.com"                 -> invariato (le email non sono menzioni)

    if not text:
        return ""
    
    text = _MENTION.sub("@user", text)      # prima le menzioni: "@nome.bsky.social" non e' un link
    text = _URL_SCHEME.sub("http", text)
    text = _URL_BARE.sub("http", text)

    return text

def quant_mode(value) -> "str | None":
    # Normalizza INFER_QUANTIZE: None = modello fp32, altrimenti il nome di una variante.
    # "true" resta valido e vuol dire "int8" (il comportamento precedente).
    if value is None or value is False:
        return None
    if value is True:
        return "int8"
    v = str(value).strip().lower()
    if v in ("", "false", "0", "no", "fp32", "none"):
        return None
    if v in ("true", "1", "yes"):
        return "int8"
    if v in QUANT_MODES:
        return v
    raise ValueError(f"Quantizzazione sconosciuta: {value!r}. Valori ammessi: false, {', '.join(QUANT_MODES)}")


def parse_engine(spec) -> Tuple[str, "str | None", str]:
    # "torch", "torch:int8pc", "ov:int8", "ov:fp32@GPU" -> (backend, variante, device)
    text = "" if spec is None else str(spec).strip()
    if text.lower() in ("", "torch", "pytorch", "fp32", "false"):
        return ("torch", None, "CPU")
    backend, _, rest = text.partition(":")
    variant, _, device = rest.partition("@")
    backend, variant, device = backend.strip().lower(), variant.strip().lower(), device.strip().upper() or "CPU"
    if backend in ("torch", "pytorch"):
        if device != "CPU":
            raise ValueError(f"Motore {spec!r}: PyTorch qui gira solo su CPU")
        return ("torch", quant_mode(variant), "CPU")
    if backend in ("ov", "openvino"):
        variant = variant or "fp32"
        if variant not in OV_VARIANTS:
            raise ValueError(f"Motore {spec!r}: variante OpenVINO sconosciuta. Valori: {', '.join(OV_VARIANTS)}")
        if device not in OV_DEVICES:
            raise ValueError(f"Motore {spec!r}: device sconosciuto. Valori: {', '.join(OV_DEVICES)}")
        return ("ov", variant, device)
    raise ValueError(f"Motore sconosciuto: {spec!r}. Esempi: torch, torch:int8pc, ov:fp32, ov:int8, ov:int8@GPU")


def engine_name(spec) -> str:
    # Forma canonica del motore: e' anche il suffisso del campo `model`
    backend, variant, device = parse_engine(spec)
    if backend == "torch":
        return "torch" if variant is None else f"torch:{variant}"
    return f"ov:{variant}" + ("" if device == "CPU" else f"@{device}")


def ov_model_path(ov_dir: str, variant: str) -> str:
    return os.path.join(ov_dir, variant, "model.xml")


def check_engine(spec, ov_dir: "str | None") -> str:
    # Da chiamare all'avvio (driver, benchmark): un motore OpenVINO senza modello esportato
    # deve fermare tutto subito, non al primo micro-batch dentro un worker Python.
    name = engine_name(spec)
    backend, variant, _ = parse_engine(name)
    if backend == "ov":
        if not ov_dir:
            raise ValueError("Motore OpenVINO senza SENTIMENT_OV_DIR")
        path = ov_model_path(ov_dir, variant)
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} non esiste: esporta prima la variante con ov_export.py")
    return name


def _quantize(model, mode: str):
    import torch
    from torch.ao.quantization import (default_dynamic_qconfig, per_channel_dynamic_qconfig,
                                       quantize_dynamic)

    per_channel, skip = QUANT_MODES[mode]
    qconfig = per_channel_dynamic_qconfig if per_channel else default_dynamic_qconfig
    skip_re = [re.compile(p) for p in skip]
    # Configurazione per nome di modulo: solo i Linear non esclusi vengono quantizzati
    spec = {
        name: qconfig
        for name, module in model.named_modules()
        if isinstance(module, torch.nn.Linear) and not any(r.search(name) for r in skip_re)
    }
    # inplace: i layer quantizzati sostituiscono quelli fp32, senza tenere in memoria
    # una seconda copia del modello
    return quantize_dynamic(model, qconfig_spec=spec, inplace=True)


def _set_torch_threads(num_threads: int):
    import torch

    # Thread intra-op (quelli che fanno i calcoli): i core assegnati al task.
    # Con piu' worker Python in parallelo, se ognuno usasse tutti i thread
    # della CPU si pesterebbero i piedi.
    torch.set_num_threads(num_threads)
    # Thread inter-op (operatori indipendenti in parallelo): l'inferenza di un
    # transformer e' una catena sequenziale, quindi non servono. Il default
    # (= core logici) creerebbe solo thread inutili. Si puo' impostare una
    # volta sola per processo, prima di qualunque calcolo.
    try:
        torch.set_num_interop_threads(1)
    except RuntimeError:
        pass


def _load_torch(model_path: str, num_threads: int, variant):
    # Restituisce run(enc) -> probabilita' (numpy, righe = testi)
    import torch
    from transformers import AutoModelForSequenceClassification
    from transformers.utils import logging as hf_logging

    _set_torch_threads(num_threads)
    hf_logging.set_verbosity_error()   # nasconde l'avviso sul pooler, che la testa di classificazione non usa
    model = AutoModelForSequenceClassification.from_pretrained(model_path, local_files_only=True)
    model.eval()

    # Quantizzazione dinamica int8 dei layer lineari (il grosso dei calcoli di RoBERTa):
    # pesi in int8, attivazioni quantizzate al volo. Su CPU piu' veloce, con risultati
    # diversi dal fp32: quanto diversi dipende dalla variante (vedi QUANT_MODES).
    # In torch 2.9 l'API puo' stampare un avviso di deprecazione: funziona comunque.
    if variant:
        model = _quantize(model, variant)

    def run(enc):
        with torch.inference_mode():
            tensors = {k: torch.from_numpy(v) for k, v in enc.items()}
            return torch.softmax(model(**tensors).logits, dim=-1).numpy()

    return run


def _softmax(logits):
    import numpy as np

    z = logits - logits.max(axis=-1, keepdims=True)
    e = np.exp(z)
    return e / e.sum(axis=-1, keepdims=True)


def _load_openvino(num_threads: int, variant: str, device: str, ov_dir: str):
    import openvino as ov

    path = ov_model_path(ov_dir, variant)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} non esiste: esporta prima la variante con ov_export.py")

    core = ov.Core()
    if device == "GPU":
        if "GPU" not in core.available_devices:
            raise RuntimeError(f"GPU non visibile a OpenVINO (dispositivi: {core.available_devices})")
        # Kernel GPU compilati salvati su disco: dal secondo avvio non si ricompilano
        core.set_property({"CACHE_DIR": os.path.join(ov_dir, "cache")})
        config = {"PERFORMANCE_HINT": "LATENCY"}
    else:
        # Un solo flusso con num_threads thread, come un task Spark. Senza pinning: piu'
        # processi OpenVINO in parallelo si bloccherebbero tutti sugli stessi core.
        config = {"INFERENCE_NUM_THREADS": int(num_threads), "NUM_STREAMS": 1, "ENABLE_CPU_PINNING": False}
    try:
        compiled = core.compile_model(path, device, config)
    except RuntimeError:
        if device != "CPU":
            raise
        compiled = core.compile_model(path, device, {"INFERENCE_NUM_THREADS": int(num_threads)})

    request = compiled.create_infer_request()
    output = compiled.output(0)

    def run(enc):
        return _softmax(request.infer(enc)[output])

    return run


def _pad_for_gpu(enc: dict, batch_size: int, pad_id: int) -> dict:
    # Lunghezza arrotondata al bucket successivo, batch completato ripetendo la prima riga
    # (righe in piu' scartate dopo l'inferenza): la GPU vede sempre poche forme fisse.
    import numpy as np

    ids, mask = enc["input_ids"], enc["attention_mask"]
    rows, length = ids.shape
    target = next((b for b in GPU_SEQ_BUCKETS if b >= length), length)
    if target > length:
        ids = np.pad(ids, ((0, 0), (0, target - length)), constant_values=pad_id)
        mask = np.pad(mask, ((0, 0), (0, target - length)), constant_values=0)
    if rows < batch_size:
        ids = np.concatenate([ids, np.repeat(ids[:1], batch_size - rows, axis=0)])
        mask = np.concatenate([mask, np.repeat(mask[:1], batch_size - rows, axis=0)])
    return {"input_ids": ids, "attention_mask": mask}


# Restituisce (tokenizer, run, labels, pad_per_gpu)
def load(model_path: str, num_threads: int, engine="torch", ov_dir: "str | None" = None):
    name = engine_name(engine)
    backend, variant, device = parse_engine(name)
    with _lock:                             # Impedisce che i threads carichino il modello contemporaneamente
        key = (model_path, name, ov_dir)
        bundle = _cache.get(key)            # Cerca il modello in cache

        if bundle is None:
            from transformers import AutoConfig, AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
            if backend == "torch":
                run = _load_torch(model_path, num_threads, variant)
            else:
                run = _load_openvino(num_threads, variant, device, ov_dir)

            # Le etichette si leggono dalla config del modello, non si assumono
            config = AutoConfig.from_pretrained(model_path, local_files_only=True)
            id2label = {int(k): str(v).lower() for k, v in config.id2label.items()}
            labels = [id2label[i] for i in range(len(id2label))]
            if set(labels) != EXPECTED_LABELS:
                raise ValueError(f"Etichette inattese nel modello {model_path}: {labels}")

            bundle = (tokenizer, run, labels, device == "GPU")
            _cache[key] = bundle                        # Carichiamo il modello in cache, così non lo carichiamo dal disco ogni volta

        return bundle


# Restituisce il sentiment e la label.
def score(texts: Sequence[str], model_path: str, batch_size: int = 32, max_length: int = 256, num_threads: int = 1,
          engine="torch", ov_dir: "str | None" = None) -> Tuple[List[float], List[str]]:
    tokenizer, run, labels, pad_for_gpu = load(model_path, num_threads, engine, ov_dir)
    i_neg, i_pos = labels.index("negative"), labels.index("positive")

    clean = [preprocess(t or "") for t in texts]
    n = len(clean)
    sentiments: List[float] = [0.0] * n
    out_labels: List[str] = [""] * n

    # Ordino per lunghezza: i batch hanno testi simili e il padding costa meno
    order = sorted(range(n), key=lambda i: len(clean[i]))

    for start in range(0, n, batch_size):
        idx = order[start:start + batch_size]
        enc = tokenizer(
            [clean[i] for i in idx],
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="np",
        )
        enc = {"input_ids": enc["input_ids"], "attention_mask": enc["attention_mask"]}
        if pad_for_gpu:
            enc = _pad_for_gpu(enc, batch_size, tokenizer.pad_token_id)
        probs = run(enc)[: len(idx)]                 # le righe aggiunte per la GPU si scartano
        s = (probs[:, i_pos] - probs[:, i_neg]).tolist()
        top = probs.argmax(axis=-1).tolist()
        for j, i in enumerate(idx):
            sentiments[i] = float(s[j])
            out_labels[i] = labels[top[j]]

    return sentiments, out_labels