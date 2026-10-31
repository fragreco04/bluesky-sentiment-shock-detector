"""
Caricamento e inferenza del modello di sentiment, lato executor.

Perche' un modulo separato e non codice dentro il job:
le variabili globali di un modulo *importato* restano vive per tutta la vita
del processo Python worker, che Spark riusa tra un task e l'altro
(spark.python.worker.reuse=true, il default). Cosi' il modello viene caricato
una volta per worker e non a ogni micro-batch. Le globali definite nello script
principale invece vengono ri-serializzate con la UDF a ogni task, quindi una
cache messa li' si svuoterebbe ogni volta.
"""
from __future__ import annotations

import re
import threading
from typing import List, Sequence, Tuple

_lock = threading.Lock()
_cache: dict = {}

EXPECTED_LABELS = {"negative", "neutral", "positive"}


# ---------------------------------------------------------------------------
# Normalizzazione del testo prima dell'inferenza
# La model card chiede menzioni -> "@user" e link -> "http", come nei tweet di
# addestramento. Su Bluesky pero':
#   - le menzioni sono handle-dominio (@nome.bsky.social), spesso attaccate a
#     punteggiatura o a capo, quindi lo split sugli spazi della model card non basta;
#   - i link nel testo sono spesso accorciati e SENZA schema
#     ("github.com/apache/spark/pul..."), quindi "inizia con http" non li prende.
# La normalizzazione serve solo al modello: `text` originale va in uscita intatto.
# ---------------------------------------------------------------------------

# @handle: non preceduta da lettera o @ (esclude le email), finisce su un carattere
# di parola (il punto finale di una frase resta fuori)
_MENTION = re.compile(r"(?<![\w@])@\w(?:[\w.-]*\w)?")

# Coda di un link: non ingloba la punteggiatura finale ma include i "..." del troncamento
_TAIL = r"(?:[^\s<>\"']*[^\s<>\"'.,;:!?)\]}])?(?:\.\.\.|…)?"

# Link con schema
_URL_SCHEME = re.compile(r"(?i)\bhttps?://" + _TAIL)

# Link senza schema. Per non scambiare "fine.Then" o "e.g." per un dominio,
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


def preprocess(text: str) -> str:
    """Menzioni -> "@user", link (con o senza schema, anche accorciati) -> "http".

    Esempi:
      "Grande post di @nome.bsky.social, leggete!" -> "Grande post di @user, leggete!"
      "Read this: github.com/apache/spark/pul... ok" -> "Read this: http ok"
      "mail me at mario@example.com"                 -> invariato (le email non sono menzioni)
    """
    if not text:
        return ""
    text = _MENTION.sub("@user", text)      # prima le menzioni: "@nome.bsky.social" non e' un link
    text = _URL_SCHEME.sub("http", text)
    text = _URL_BARE.sub("http", text)
    return text


def load(model_path: str, num_threads: int):
    """Restituisce (tokenizer, model, labels); carica dal disco solo la prima volta."""
    with _lock:
        bundle = _cache.get(model_path)
        if bundle is None:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            # Con N core per executor girano N worker Python in parallelo:
            # se ognuno usasse tutti i thread della CPU si pesterebbero i piedi.
            torch.set_num_threads(num_threads)

            tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
            model = AutoModelForSequenceClassification.from_pretrained(
                model_path, local_files_only=True
            )
            model.eval()

            # Le etichette si leggono dalla config del modello, non si assumono
            id2label = {int(k): str(v).lower() for k, v in model.config.id2label.items()}
            labels = [id2label[i] for i in range(len(id2label))]
            if set(labels) != EXPECTED_LABELS:
                raise ValueError(f"Etichette inattese nel modello {model_path}: {labels}")

            bundle = (tokenizer, model, labels)
            _cache[model_path] = bundle
        return bundle


def score(
    texts: Sequence[str],
    model_path: str,
    batch_size: int = 32,
    max_length: int = 256,
    num_threads: int = 1,
) -> Tuple[List[float], List[str]]:
    """Per ogni testo: sentiment = p_pos - p_neg in [-1, 1] e label con probabilita' massima."""
    import torch

    tokenizer, model, labels = load(model_path, num_threads)
    i_neg, i_pos = labels.index("negative"), labels.index("positive")

    clean = [preprocess(t or "") for t in texts]
    n = len(clean)
    sentiments: List[float] = [0.0] * n
    out_labels: List[str] = [""] * n

    # Ordino per lunghezza: i batch hanno testi simili e il padding costa meno
    order = sorted(range(n), key=lambda i: len(clean[i]))

    with torch.inference_mode():
        for start in range(0, n, batch_size):
            idx = order[start:start + batch_size]
            enc = tokenizer(
                [clean[i] for i in idx],
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            )
            probs = torch.softmax(model(**enc).logits, dim=-1)
            s = (probs[:, i_pos] - probs[:, i_neg]).tolist()
            top = probs.argmax(dim=-1).tolist()
            for j, i in enumerate(idx):
                sentiments[i] = float(s[j])
                out_labels[i] = labels[top[j]]

    return sentiments, out_labels