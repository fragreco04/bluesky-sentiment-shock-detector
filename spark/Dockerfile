# Tutti i valori arrivano dal .env, tramite i build args del compose: nessun default qui.
ARG SPARK_VERSION
ARG SCALA_VERSION
# Immagine ufficiale, variante python3 (Ubuntu 22.04, Python 3.10)
FROM spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java17-python3-ubuntu

USER root

# Le versioni delle librerie stanno in requirements.txt.
# torch si installa prima, dall'indice CPU di PyTorch, alla versione scritta li';
# il secondo pip install lo trova gia' soddisfatto e non scarica la build CUDA da PyPI.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
        "$(grep -E '^torch==' /tmp/requirements.txt | cut -d' ' -f1)" \
 && pip install --no-cache-dir -r /tmp/requirements.txt

ARG SENTIMENT_MODEL_NAME
ARG SENTIMENT_MODEL_PATH
ARG SPARK_CHECKPOINT_DIR

# Se un valore manca nel .env la build si ferma, invece di partire con una stringa vuota.
# Poi il modello viene scaricato UNA volta e finisce nell'immagine:
# driver ed executor lo trovano gia' su disco, senza rete e senza download all'avvio.
RUN for v in SENTIMENT_MODEL_NAME SENTIMENT_MODEL_PATH SPARK_CHECKPOINT_DIR; do \
        [ -n "$(printenv "$v")" ] || { echo "$v mancante nel .env" >&2; exit 1; }; \
    done; \
    python3 -c "from huggingface_hub import snapshot_download; \
snapshot_download(repo_id='${SENTIMENT_MODEL_NAME}', \
local_dir='${SENTIMENT_MODEL_PATH}', \
allow_patterns=['*.json', '*.txt', '*.bin', '*.safetensors'])" \
 && chmod -R a+rX "${SENTIMENT_MODEL_PATH}" \
 && mkdir -p "${SPARK_CHECKPOINT_DIR}" && chown -R spark:spark "${SPARK_CHECKPOINT_DIR}"

# A runtime nessuno deve contattare Hugging Face
ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

USER spark