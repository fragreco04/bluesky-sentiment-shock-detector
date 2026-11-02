"""
Q3 - Detector degli shock di sentiment.

sentiment.1m (JSON) -> parsing -> stato per model (applyInPandasWithState):
                       deduplica su window_start, buchi, finestre parziali e controlli di qualita',
                       baseline EWMA winsorizzata di sentiment e log-volume, z-score, episodi
                    -> sentiment.monitor (JSON, key = window_start, una riga per ogni finestra)
                    -> alerts            (JSON, key = alert_id, un messaggio all'apertura e uno alla chiusura)

Il detector e' guidato solo dall'event time (window_start), mai dall'orologio:
rielaborare lo stesso topic con gli stessi parametri produce esattamente gli stessi output.
"""
import json
import math
import os
from typing import Iterator, Optional, Tuple

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

# Dal .env, tramite il compose
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
INPUT_TOPIC = os.environ["INPUT_TOPIC"]                    # sentiment.1m
MONITOR_TOPIC = os.environ["MONITOR_TOPIC"]                # sentiment.monitor
ALERTS_TOPIC = os.environ["ALERTS_TOPIC"]                  # alerts
STARTING_OFFSETS = os.environ["STARTING_OFFSETS"]
MAX_OFFSETS_PER_TRIGGER = os.environ["MAX_OFFSETS_PER_TRIGGER"]
TRIGGER_INTERVAL = os.environ["TRIGGER_INTERVAL"]
SHUFFLE_PARTITIONS = os.environ["SHUFFLE_PARTITIONS"]      # 1: c'e' una sola chiave (model); fissato nel checkpoint
CHECKPOINT_ROOT = os.environ["CHECKPOINT_ROOT"]

# Parametri del detector: definiti solo qui, sovrascrivibili da ambiente per gli esperimenti.
# Valori provvisori, da calibrare su piu' giorni di sentiment.1m.
ALPHA = float(os.getenv("DET_ALPHA", "0.03"))                  # EWMA: dimezzamento ~23 finestre
K = float(os.getenv("DET_K", "3.5"))                           # shock forte: |z_sentiment| > K
K_LOW = float(os.getenv("DET_K_LOW", "2.5"))                   # shock moderato...
K_VOL = float(os.getenv("DET_K_VOL", "4.0"))                   # ...confermato da z_volume > K_VOL
CLIP_K = float(os.getenv("DET_CLIP_K", str(K)))                # winsorizzazione: delta tagliato a +-CLIP_K * sigma
SIGMA_MIN_SENT = float(os.getenv("DET_SIGMA_MIN_SENT", "0.01"))
SIGMA_MIN_VOL = float(os.getenv("DET_SIGMA_MIN_VOL", "0.05"))  # sul log-volume: con K_VOL=4, mai sotto +22%
MIN_POSTS = int(os.getenv("DET_MIN_POSTS", "300"))             # errore di campionamento della media <= ~0.03
MIN_AUTHORS = int(os.getenv("DET_MIN_AUTHORS", "250"))
R_MIN = float(os.getenv("DET_R_MIN", "0.8"))                   # n_authors / n_posts: normale ~0.9
PARTIAL_RATIO = float(os.getenv("DET_PARTIAL_RATIO", "0.5"))   # sotto meta' del volume tipico: finestra incompleta
WARMUP_N = int(os.getenv("DET_WARMUP_N", "100"))               # ~3/ALPHA finestre valide
CLOSE_AFTER = int(os.getenv("DET_CLOSE_AFTER", "5"))           # finestre normali consecutive per chiudere un episodio
GAP_RESET_MINUTES = float(os.getenv("DET_GAP_RESET_MINUTES", "60"))  # buco oltre il quale si rifa' il warmup

# Copiati in ogni alert: lo rendono riproducibile
PARAMS = {
    "alpha": ALPHA, "k": K, "k_low": K_LOW, "k_vol": K_VOL, "clip_k": CLIP_K,
    "sigma_min_sentiment": SIGMA_MIN_SENT, "sigma_min_volume": SIGMA_MIN_VOL,
    "min_posts": MIN_POSTS, "min_authors": MIN_AUTHORS, "r_min": R_MIN,
    "partial_ratio": PARTIAL_RATIO, "warmup_n": WARMUP_N,
    "close_after": CLOSE_AFTER, "gap_reset_minutes": GAP_RESET_MINUTES,
}

# Il checkpoint dipende da input e output: un esperimento con parametri diversi scrive su
# topic diversi (alerts.k4, ...) e quindi riparte da zero sul suo storico, senza riusare lo stato.
CHECKPOINT = f"{CHECKPOINT_ROOT}/q3_detect/{INPUT_TOPIC}/{ALERTS_TOPIC}"

SENTIMENT_SCHEMA = StructType([
    StructField("window_start", StringType(), True),
    StructField("window_end", StringType(), True),
    StructField("n_posts", LongType(), True),
    StructField("n_authors", LongType(), True),
    StructField("mean_sentiment", DoubleType(), True),
    StructField("std_sentiment", DoubleType(), True),
    StructField("share_negative", DoubleType(), True),
    StructField("share_positive", DoubleType(), True),
    StructField("model", StringType(), True),
])

# Stato per chiave (model). Nessun null: stringhe vuote e zeri quando un campo non ha senso.
STATE_FIELDS = [
    ("last_start", StringType()),        # ultima finestra elaborata: deduplica e buchi
    ("last_end", StringType()),
    ("has_baseline", BooleanType()),     # mu e var inizializzate
    ("n_valid", LongType()),             # finestre valide dall'ultimo (ri)avvio del warmup
    ("mu_s", DoubleType()), ("var_s", DoubleType()),     # EWMA del sentiment medio
    ("mu_v", DoubleType()), ("var_v", DoubleType()),     # EWMA di log(1 + n_posts)
    ("ep_open", BooleanType()),          # episodio in corso
    ("ep_id", StringType()),
    ("ep_start", StringType()),
    ("ep_end", StringType()),            # fine dell'ultima finestra anomala
    ("ep_direction", StringType()),
    ("ep_streak", LongType()),           # finestre normali consecutive
    ("ep_n_shock", LongType()),          # finestre anomale nell'episodio
    ("ep_peak_z", DoubleType()),
    ("ep_peak_window", StringType()),
    ("ep_peak_mean", DoubleType()),
    ("ep_peak_n_posts", LongType()),
    ("ep_peak_z_volume", DoubleType()),
    ("ep_base_mean", DoubleType()),      # baseline al momento dell'apertura
    ("ep_base_std", DoubleType()),
]
STATE_NAMES = [name for name, _ in STATE_FIELDS]
STATE_SCHEMA = StructType([StructField(name, t, False) for name, t in STATE_FIELDS])

# Un solo schema di uscita per i due topic: il JSON viene costruito qui in Python,
# kind dice dove va, sort_key + seq danno l'ordine di scrittura.
OUTPUT_SCHEMA = StructType([
    StructField("kind", StringType(), False),        # "monitor" o "alert"
    StructField("sort_key", StringType(), False),    # window_start della finestra che ha prodotto il messaggio
    StructField("seq", LongType(), False),           # ordine di emissione dentro la chiamata
    StructField("key", StringType(), False),
    StructField("value", StringType(), False),
])
OUTPUT_COLUMNS = [f.name for f in OUTPUT_SCHEMA.fields]


def new_state() -> dict:
    s = {
        "last_start": "", "last_end": "",
        "has_baseline": False, "n_valid": 0,
        "mu_s": 0.0, "var_s": 0.0, "mu_v": 0.0, "var_v": 0.0,
    }
    reset_episode(s)
    return s


def reset_episode(s: dict) -> None:
    s.update({
        "ep_open": False, "ep_id": "", "ep_start": "", "ep_end": "", "ep_direction": "",
        "ep_streak": 0, "ep_n_shock": 0,
        "ep_peak_z": 0.0, "ep_peak_window": "", "ep_peak_mean": 0.0,
        "ep_peak_n_posts": 0, "ep_peak_z_volume": 0.0,
        "ep_base_mean": 0.0, "ep_base_std": 0.0,
    })


# Restituisce un float JSON-compatibile: NaN (es. std_sentiment con un solo post) -> null
def num(v) -> Optional[float]:
    if v is None:
        return None
    v = float(v)
    return None if math.isnan(v) else v


def sigma(var: float, floor: float) -> float:
    # La soglia minima evita z enormi quando un periodo molto stabile porta la varianza verso zero
    return max(math.sqrt(var), floor)


def ewma(mu: float, var: float, x: float, clip: Optional[float]) -> Tuple[float, float]:
    # delta calcolato con la media vecchia e usato in entrambe le formule.
    # Winsorizzazione: con clip, un valore estremo sposta la baseline al massimo di ALPHA * clip,
    # cosi' uno shock non la contamina, ma un cambio di livello duraturo viene assorbito.
    d = x - mu
    if clip is not None:
        d = max(-clip, min(clip, d))
    return mu + ALPHA * d, (1 - ALPHA) * (var + ALPHA * d * d)


def alert_message(s: dict, model: str, status: str, close_reason: Optional[str] = None) -> dict:
    msg = {
        "alert_id": s["ep_id"],
        "status": status,
        "start": s["ep_start"],
        "direction": s["ep_direction"],
        "peak_z": s["ep_peak_z"],
        "peak_window": s["ep_peak_window"],
        "peak_mean_sentiment": s["ep_peak_mean"],
        "baseline_mean": s["ep_base_mean"],
        "baseline_std": s["ep_base_std"],
        "peak_n_posts": s["ep_peak_n_posts"],
        "z_volume": s["ep_peak_z_volume"],
        "n_shock_windows": s["ep_n_shock"],
        "model": model,
        "params": PARAMS,
    }
    if status == "closed":
        msg["end"] = s["ep_end"]
        msg["close_reason"] = close_reason      # normal, reversal o gap
    return msg


def detect(key: Tuple[str], pdf_iter: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    """Chiamata da Spark a ogni micro-batch per ogni model con righe nuove.

    Riceve le finestre arrivate e lo stato salvato alla fine del batch precedente,
    elabora le finestre in ordine di window_start e restituisce i messaggi per i due topic.
    """
    model = key[0]
    pdf = pd.concat(list(pdf_iter), ignore_index=True)
    if pdf.empty:
        return

    # Spark non garantisce l'ordine delle righe dentro il gruppo.
    # Le stringhe ISO a formato fisso si ordinano come i tempi che rappresentano.
    pdf = pdf.sort_values("window_start", kind="stable")

    s = dict(zip(STATE_NAMES, state.get)) if state.exists else new_state()
    out = []

    def emit(kind: str, sort_key: str, k: str, payload: dict) -> None:
        out.append((kind, sort_key, len(out), k, json.dumps(payload, ensure_ascii=False)))

    def close_episode(reason: str, sort_key: str) -> None:
        emit("alert", sort_key, s["ep_id"], alert_message(s, model, "closed", reason))
        reset_episode(s)

    for row in pdf.itertuples(index=False):
        ws, we = row.window_start, row.window_end
        n_posts, n_authors = int(row.n_posts), int(row.n_authors)
        x_s = float(row.mean_sentiment)
        x_v = math.log1p(n_posts)

        # 1) Duplicati: Q2 e' at-least-once e dopo un crash puo' riemettere finestre gia' viste.
        #    Senza questo controllo aggiornerebbero l'EWMA due volte. Nessun output.
        if s["last_start"] and ws <= s["last_start"]:
            continue

        # 2) Buchi: finestra non consecutiva = interruzione dell'ingestione, non un crollo.
        first = not s["last_start"]
        after_gap = (not first) and ws != s["last_end"]
        if after_gap:
            gap = pd.Timestamp(ws) - pd.Timestamp(s["last_end"])
            gap_windows = gap / (pd.Timestamp(we) - pd.Timestamp(ws))
            # Un episodio non sopravvive a un buco lungo quanto la sua chiusura
            if s["ep_open"] and gap_windows >= CLOSE_AFTER:
                close_episode("gap", ws)
            # Dopo un buco lungo la baseline e' vecchia (altra fascia oraria): si rifa' il warmup,
            # partendo da mu e var attuali invece che da zero
            if gap.total_seconds() / 60 >= GAP_RESET_MINUTES:
                s["n_valid"] = 0
        s["last_start"], s["last_end"] = ws, we

        # 3) Baseline e z-score, calcolati con lo stato PRIMA dell'aggiornamento
        if s["has_baseline"]:
            base_mean = s["mu_s"]
            base_std = sigma(s["var_s"], SIGMA_MIN_SENT)
            z_s = (x_s - base_mean) / base_std
            base_vol = math.expm1(s["mu_v"])                  # volume tipico in post per finestra
            z_v = (x_v - s["mu_v"]) / sigma(s["var_v"], SIGMA_MIN_VOL)
        else:
            base_mean = base_std = z_s = base_vol = z_v = None

        # 4) Controlli di qualita'. Una finestra scartata produce comunque la riga di monitor,
        #    ma non aggiorna la baseline e non conta per gli episodi.
        ratio = n_authors / n_posts if n_posts else 0.0
        if first:
            skip_reason = "startup"            # la prima finestra dopo l'avvio e' parziale
        elif after_gap:
            skip_reason = "after_gap"          # anche la prima dopo un buco
        elif s["has_baseline"] and n_posts < PARTIAL_RATIO * base_vol:
            skip_reason = "partial"            # es. l'ultima finestra prima di un'interruzione
        elif n_posts < MIN_POSTS:
            skip_reason = "low_volume"         # media troppo rumorosa
        elif n_authors < MIN_AUTHORS:
            skip_reason = "low_authors"
        elif ratio < R_MIN:
            skip_reason = "low_diversity"      # pochi autori producono molti post: probabile spam
        else:
            skip_reason = None
        valid = skip_reason is None

        warmup = s["n_valid"] < WARMUP_N
        active = valid and not warmup and s["has_baseline"]

        # 5) Regola di shock: forte sul solo sentiment, oppure moderato confermato dal volume.
        #    Un picco di volume con sentiment normale non e' uno shock: solo volume_spike.
        is_shock = volume_spike = False
        direction = None
        if active:
            is_shock = abs(z_s) > K or (abs(z_s) > K_LOW and z_v > K_VOL)
            volume_spike = z_v > K_VOL
            direction = "negative" if z_s < 0 else "positive"

        # 6) Episodi
        if is_shock:
            # Un episodio ha una sola direzione: un'anomalia opposta chiude e ne apre uno nuovo
            if s["ep_open"] and s["ep_direction"] != direction:
                close_episode("reversal", ws)
            if not s["ep_open"]:
                s.update({
                    "ep_open": True,
                    "ep_id": f"shock-{ws}",    # deterministico: una rielaborazione produce lo stesso id
                    "ep_start": ws, "ep_end": we, "ep_direction": direction,
                    "ep_streak": 0, "ep_n_shock": 1,
                    "ep_peak_z": z_s, "ep_peak_window": ws, "ep_peak_mean": x_s,
                    "ep_peak_n_posts": n_posts, "ep_peak_z_volume": z_v,
                    "ep_base_mean": base_mean, "ep_base_std": base_std,
                })
                emit("alert", ws, s["ep_id"], alert_message(s, model, "open"))
            else:
                s["ep_end"] = we
                s["ep_streak"] = 0
                s["ep_n_shock"] += 1
                if abs(z_s) > abs(s["ep_peak_z"]):
                    s.update({
                        "ep_peak_z": z_s, "ep_peak_window": ws, "ep_peak_mean": x_s,
                        "ep_peak_n_posts": n_posts, "ep_peak_z_volume": z_v,
                    })

        # L'alert_id copre tutto l'episodio, comprese le finestre normali in attesa di chiusura
        alert_id = s["ep_id"] if s["ep_open"] else None

        # Le finestre scartate non contano ne' come anomale ne' come normali
        if active and not is_shock and s["ep_open"]:
            s["ep_streak"] += 1
            if s["ep_streak"] >= CLOSE_AFTER:
                close_episode("normal", ws)

        # 7) Aggiornamento della baseline, solo con finestre valide.
        #    Nel warmup niente winsorizzazione: la varianza parte da 0 e taglierebbe tutto.
        if valid:
            if not s["has_baseline"]:
                s.update({"mu_s": x_s, "var_s": 0.0, "mu_v": x_v, "var_v": 0.0, "has_baseline": True})
            else:
                clip_s = None if warmup else CLIP_K * sigma(s["var_s"], SIGMA_MIN_SENT)
                clip_v = None if warmup else CLIP_K * sigma(s["var_v"], SIGMA_MIN_VOL)
                s["mu_s"], s["var_s"] = ewma(s["mu_s"], s["var_s"], x_s, clip_s)
                s["mu_v"], s["var_v"] = ewma(s["mu_v"], s["var_v"], x_v, clip_v)
            s["n_valid"] += 1

        # 8) Riga di monitor: tutti i campi di sentiment.1m piu' lo stato del detector
        emit("monitor", ws, ws, {
            "window_start": ws,
            "window_end": we,
            "n_posts": n_posts,
            "n_authors": n_authors,
            "author_ratio": ratio,
            "mean_sentiment": x_s,
            "std_sentiment": num(row.std_sentiment),
            "share_negative": num(row.share_negative),
            "share_positive": num(row.share_positive),
            "model": model,
            "baseline_mean": base_mean,
            "baseline_std": base_std,
            "z_sentiment": z_s,
            "baseline_volume": base_vol,
            "z_volume": z_v,
            "warmup": warmup,
            "is_shock": is_shock,
            "volume_spike": volume_spike,
            "skip_reason": skip_reason,
            "alert_id": alert_id,
        })

    state.update(tuple(s[name] for name in STATE_NAMES))
    if out:
        yield pd.DataFrame(out, columns=OUTPUT_COLUMNS)


def write_batch_to_kafka(batch_df, batch_id):
    """Scrive i messaggi del micro-batch sui due topic, ciascuno in ordine.

    Come in Q2: un solo task che ordina prima di scrivere, perche' il sink Kafka diretto
    scriverebbe con task paralleli in ordine arbitrario.

    Garanzia at-least-once: se il job cade dopo la scrittura e prima del commit, il batch
    viene rieseguito dallo stesso stato e produce gli stessi messaggi. Elasticsearch usa
    window_start e alert_id come _id, quindi le copie sovrascrivono gli originali.
    """
    batch_df.persist()                  # letto due volte: una per topic
    try:
        for kind, topic in (("monitor", MONITOR_TOPIC), ("alert", ALERTS_TOPIC)):
            (
                batch_df
                .filter(F.col("kind") == kind)
                .repartition(1)
                .sortWithinPartitions("sort_key", "seq")
                .select("key", "value")
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("topic", topic)
                .save()
            )
    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("q3_detect")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Sorgente Kafka: 1 partizione, finestre gia' in ordine di tempo
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Parsing. Righe incomplete scartate; std e share possono essere null e passano invariati.
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), SENTIMENT_SCHEMA).alias("p"))
        .select("p.*")
        .filter(
            F.col("window_start").isNotNull()
            & F.col("window_end").isNotNull()
            & F.col("n_posts").isNotNull()
            & F.col("n_authors").isNotNull()
            & F.col("mean_sentiment").isNotNull()
            & F.col("model").isNotNull()
        )
    )

    # 3) Detector. Chiave = model: una baseline per modello, e un cambio di modello riparte
    #    da uno stato nuovo con il suo warmup. Nessun timeout: il detector avanza solo quando
    #    arrivano finestre, quindi non serve un watermark.
    detected = (
        parsed
        .groupBy("model")
        .applyInPandasWithState(
            detect,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    # 4) Scrittura sui due topic
    query = (
        detected.writeStream
        .queryName("q3_detect")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(write_batch_to_kafka)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()