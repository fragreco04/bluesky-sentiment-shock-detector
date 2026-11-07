"""
Esporta il modello di sentiment in formato OpenVINO (IR), variante fp32: quella usata da Q1
(INFER_ENGINE=ov:fp32@GPU). Stesso calcolo di PyTorch, eseguito dal motore OpenVINO.

Il risultato finisce in SENTIMENT_OV_DIR/fp32/model.xml (+ model.bin), nel volume sentiment-ov:
si esporta una volta e resta tra i riavvii.

Uso (dentro l'immagine Spark, con Q1 fermo: servono 2-3 GB di RAM):
  docker compose run --rm --no-deps spark-driver-q1 python3 /opt/spark/jobs/ov_export.py
"""
import os
import shutil
import sys

INPUT_NAMES = ("input_ids", "attention_mask")


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

    # Nomi fissi per ingressi e uscita: sentiment_model passa input_ids e attention_mask per nome
    for port, name in zip(ov_model.inputs, INPUT_NAMES):
        port.get_tensor().set_names({name})
    ov_model.outputs[0].get_tensor().set_names({"logits"})
    # Batch e lunghezza variabili: ogni micro-batch ha forme diverse
    if any(port.get_partial_shape().is_static for port in ov_model.inputs):
        ov_model.reshape({name: ov.PartialShape([-1, -1]) for name in INPUT_NAMES})
    return ov_model


def save(model, out_dir):
    import openvino as ov

    target = os.path.join(out_dir, "fp32")
    tmp = target + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    # compress_to_fp16=False: i pesi restano fp32. Con il default verrebbero arrotondati a fp16
    # su disco, cambiando i risultati anche sulla CPU.
    ov.save_model(model, os.path.join(tmp, "model.xml"), compress_to_fp16=False)
    # Si scrive in .tmp e si sposta al suo posto solo a scrittura finita:
    # un errore a meta' export lascia intatta la versione precedente.
    shutil.rmtree(target, ignore_errors=True)
    os.replace(tmp, target)
    print(f"Salvato {target}/model.xml")


def main():
    model_path = os.getenv("SENTIMENT_MODEL_PATH")
    out_dir = os.getenv("SENTIMENT_OV_DIR")
    if not model_path or not out_dir:
        sys.exit("Servono SENTIMENT_MODEL_PATH e SENTIMENT_OV_DIR (le imposta il compose)")
    print("Conversione PyTorch -> OpenVINO fp32 ...")
    save(export_fp32(model_path), out_dir)


if __name__ == "__main__":
    main()