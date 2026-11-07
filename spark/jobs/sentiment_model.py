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

Motori di inferenza (parametro `engine`, dal .env: INFER_ENGINE), tutti sul modello fp32:
  "torch"         PyTorch su CPU: il riferimento
  "ov:fp32"       OpenVINO su CPU, con il modello esportato da ov_export.py
  "ov:fp32@GPU"   OpenVINO sulla GPU integrata (serve /dev/dxg nel container)
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

# Unica variante OpenVINO: la cartella creata da ov_export.py in SENTIMENT_OV_DIR
OV_VARIANT = "fp32"
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

def parse_engine(spec) -> Tuple[str, str]:
    # "torch", "ov:fp32", "ov:fp32@GPU" -> (backend, device). Tutti i motori usano il modello fp32.
    text = "" if spec is None else str(spec).strip()
    if text.lower() in ("", "torch", "pytorch", "fp32", "torch:fp32"):
        return ("torch", "CPU")
    backend, _, rest = text.partition(":")
    variant, _, device = rest.partition("@")
    backend, variant, device = backend.strip().lower(), variant.strip().lower(), device.strip().upper() or "CPU"
    if backend in ("ov", "openvino") and variant in ("", OV_VARIANT):
        if device not in OV_DEVICES:
            raise ValueError(f"Motore {spec!r}: device sconosciuto. Valori: {', '.join(OV_DEVICES)}")
        return ("ov", device)
    raise ValueError(f"Motore sconosciuto: {spec!r}. Valori: torch, ov:fp32, ov:fp32@GPU")


def engine_name(spec) -> str:
    # Forma canonica del motore: e' anche il suffisso del campo `model`
    backend, device = parse_engine(spec)
    if backend == "torch":
        return "torch"
    return f"ov:{OV_VARIANT}" + ("" if device == "CPU" else f"@{device}")


def ov_model_path(ov_dir: str) -> str:
    return os.path.join(ov_dir, OV_VARIANT, "model.xml")


def check_engine(spec, ov_dir: "str | None") -> str:
    # Da chiamare all'avvio (driver, benchmark): un motore OpenVINO senza modello esportato
    # deve fermare tutto subito, non al primo micro-batch dentro un worker Python.
    name = engine_name(spec)
    backend, _ = parse_engine(name)
    if backend == "ov":
        if not ov_dir:
            raise ValueError("Motore OpenVINO senza SENTIMENT_OV_DIR")
        path = ov_model_path(ov_dir)
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} non esiste: esporta prima il modello con ov_export.py")
    return name


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


def _load_torch(model_path: str, num_threads: int):
    # Restituisce run(enc) -> probabilita' (numpy, righe = testi)
    import torch
    from transformers import AutoModelForSequenceClassification
    from transformers.utils import logging as hf_logging

    _set_torch_threads(num_threads)
    hf_logging.set_verbosity_error()   # nasconde l'avviso sul pooler, che la testa di classificazione non usa
    model = AutoModelForSequenceClassification.from_pretrained(model_path, local_files_only=True)
    model.eval()


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


def _load_openvino(num_threads: int, device: str, ov_dir: str):
    import openvino as ov

    path = ov_model_path(ov_dir)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} non esiste: esporta prima il modello con ov_export.py")

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


def _warmup_gpu(run, batch_size: int, max_length: int, cls_id: int, pad_id: int):
    # Compila subito tutte le forme che la GPU vedra' (una per bucket di lunghezza): senza,
    # ogni forma nuova si compilerebbe a meta' lavoro, rallentando i primi micro-batch.
    # Con CACHE_DIR i kernel finiscono su disco: dal secondo avvio e' quasi immediato.
    import numpy as np

    last = next((b for b in GPU_SEQ_BUCKETS if b >= max_length), max_length)
    for length in [b for b in GPU_SEQ_BUCKETS if b <= last]:
        ids = np.full((batch_size, length), pad_id, dtype=np.int64)
        mask = np.zeros((batch_size, length), dtype=np.int64)
        ids[:, 0], mask[:, 0] = cls_id, 1
        run({"input_ids": ids, "attention_mask": mask})


# Restituisce (tokenizer, run, labels, stato_gpu); stato_gpu e' None sulla CPU
def load(model_path: str, num_threads: int, engine="torch", ov_dir: "str | None" = None):
    name = engine_name(engine)
    backend, device = parse_engine(name)
    with _lock:                             # Impedisce che i threads carichino il modello contemporaneamente
        key = (model_path, name, ov_dir)
        bundle = _cache.get(key)            # Cerca il modello in cache

        if bundle is None:
            from transformers import AutoConfig, AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
            if backend == "torch":
                run = _load_torch(model_path, num_threads)
            else:
                run = _load_openvino(num_threads, device, ov_dir)

            # Le etichette si leggono dalla config del modello, non si assumono
            config = AutoConfig.from_pretrained(model_path, local_files_only=True)
            id2label = {int(k): str(v).lower() for k, v in config.id2label.items()}
            labels = [id2label[i] for i in range(len(id2label))]
            if set(labels) != EXPECTED_LABELS:
                raise ValueError(f"Etichette inattese nel modello {model_path}: {labels}")

            # Sulla GPU: insieme delle forme gia' compilate per (batch_size, max_length)
            bundle = (tokenizer, run, labels, set() if device == "GPU" else None)
            _cache[key] = bundle                        # Carichiamo il modello in cache, così non lo carichiamo dal disco ogni volta

        return bundle


# Restituisce il sentiment e la label.
def score(texts: Sequence[str], model_path: str, batch_size: int = 32, max_length: int = 256, num_threads: int = 1,
          engine="torch", ov_dir: "str | None" = None) -> Tuple[List[float], List[str]]:
    tokenizer, run, labels, gpu_warm = load(model_path, num_threads, engine, ov_dir)
    pad_for_gpu = gpu_warm is not None
    i_neg, i_pos = labels.index("negative"), labels.index("positive")
    if pad_for_gpu and (batch_size, max_length) not in gpu_warm:
        with _lock:
            if (batch_size, max_length) not in gpu_warm:
                _warmup_gpu(run, batch_size, max_length, tokenizer.cls_token_id, tokenizer.pad_token_id)
                gpu_warm.add((batch_size, max_length))

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