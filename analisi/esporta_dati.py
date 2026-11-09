"""
Esporta da Elasticsearch i dati della sessione in file Parquet.

Requisiti:  pip install "elasticsearch>=9,<10" pandas pyarrow
Uso:        python esporta_dati.py
"""
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers

ES = Elasticsearch("http://localhost:9200", request_timeout=300)
OUT = Path("dati")
OUT.mkdir(exist_ok=True)

# indice -> (campi tempo da convertire, file di uscita)
INDICI = {
    "sentiment-1m": (["window_start", "window_end"], "sentiment_1m.parquet"),
    "alerts":       (["start", "end", "peak_window"], "alerts.parquet"),
    "posts-*":      (["time"], "posts.parquet"),
}


def esporta(indice, campi_tempo, nome_file):
    righe = []
    for i, hit in enumerate(helpers.scan(ES, index=indice,
                                         query={"query": {"match_all": {}}},
                                         size=5000, preserve_order=False), 1):
        righe.append(hit["_source"])
        if i % 100_000 == 0:
            print(f"  {indice}: {i:,} documenti letti")

    if not righe:
        print(f"{indice}: vuoto, salto")
        return

    df = pd.DataFrame(righe)
    for campo in campi_tempo:
        if campo in df.columns:
            df[campo] = pd.to_datetime(df[campo], utc=True, format="ISO8601")
    # Ora italiana, comoda per i grafici
    principale = campi_tempo[0]
    if principale in df.columns:
        df[principale + "_it"] = df[principale].dt.tz_convert("Europe/Rome")

    df.to_parquet(OUT / nome_file, index=False)
    print(f"{indice}: {len(df):,} righe -> {OUT / nome_file}")


if __name__ == "__main__":
    for indice, (campi, file) in INDICI.items():
        esporta(indice, campi, file)