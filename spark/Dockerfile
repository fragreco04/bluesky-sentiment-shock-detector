ARG SPARK_VERSION
ARG SCALA_VERSION
FROM spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java17-python3-ubuntu

USER root

# Runtime GPU Intel (OpenCL) per OpenVINO sulla GPU integrata. Stesse versioni dell'immagine
# ufficiale openvino/ubuntu22_dev:2026.2.1, verificata su questo PC (Iris Xe, WSL2, Docker Desktop).
# Senza GPU (o senza docker-compose.gpu.yml) questi pacchetti restano inutilizzati e non danno fastidio.
ARG INTEL_IGC_VERSION=2.2.3
ARG INTEL_IGC_BUILD=18220
ARG INTEL_NEO_VERSION=24.48.31907.7
ARG INTEL_GMM_VERSION=22.5.4
ARG INTEL_NEO_SUMS=ww48.sum
RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates wget; \
    mkdir /tmp/neo; cd /tmp/neo; \
    igc="https://github.com/intel/intel-graphics-compiler/releases/download/v${INTEL_IGC_VERSION}"; \
    neo="https://github.com/intel/compute-runtime/releases/download/${INTEL_NEO_VERSION}"; \
    wget -q "${igc}/intel-igc-core-2_${INTEL_IGC_VERSION}+${INTEL_IGC_BUILD}_amd64.deb" \
            "${igc}/intel-igc-opencl-2_${INTEL_IGC_VERSION}+${INTEL_IGC_BUILD}_amd64.deb" \
            "${neo}/intel-opencl-icd_${INTEL_NEO_VERSION}_amd64.deb" \
            "${neo}/libigdgmm12_${INTEL_GMM_VERSION}_amd64.deb" \
            "${neo}/${INTEL_NEO_SUMS}"; \
    # Controllo d'integrita' dei pacchetti del runtime (le somme dell'IGC stanno nella sua release)
    sha256sum -c --ignore-missing "${INTEL_NEO_SUMS}"; \
    # apt installa i .deb locali e ne risolve le dipendenze (es. ocl-icd-libopencl1)
    apt-get install -y --no-install-recommends ./*.deb; \
    cd /; rm -rf /tmp/neo /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
        "$(grep -E '^torch==' /tmp/requirements.txt | cut -d' ' -f1)" \
 && pip install --no-cache-dir -r /tmp/requirements.txt

ARG SENTIMENT_MODEL_NAME
ARG SENTIMENT_MODEL_PATH
ARG SENTIMENT_OV_DIR
ARG SPARK_CHECKPOINT_DIR

# Se un valore manca nel .env la build si ferma, invece di partire con una stringa vuota.
# Poi il modello viene scaricato UNA volta e finisce nell'immagine:
# driver ed executor lo trovano gia' su disco, senza rete e senza download all'avvio.
RUN for v in SENTIMENT_MODEL_NAME SENTIMENT_MODEL_PATH SENTIMENT_OV_DIR SPARK_CHECKPOINT_DIR; do \
        [ -n "$(printenv "$v")" ] || { echo "$v mancante nel .env" >&2; exit 1; }; \
    done; \
    python3 -c "from huggingface_hub import snapshot_download; \
snapshot_download(repo_id='${SENTIMENT_MODEL_NAME}', \
local_dir='${SENTIMENT_MODEL_PATH}', \
allow_patterns=['*.json', '*.txt', '*.bin', '*.safetensors'])" \
 && chmod -R a+rX "${SENTIMENT_MODEL_PATH}" \
 && mkdir -p "${SPARK_CHECKPOINT_DIR}" "${SENTIMENT_OV_DIR}" \
 && chown -R spark:spark "${SPARK_CHECKPOINT_DIR}" "${SENTIMENT_OV_DIR}"
# SENTIMENT_OV_DIR e' montato come volume: al primo avvio Docker lo crea con il proprietario
# di questa cartella (spark), cosi' ov_export.py puo' scriverci senza permessi di root

# A runtime nessuno deve contattare Hugging Face
ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

USER spark