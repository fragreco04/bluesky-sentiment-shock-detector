"""
Producer sintetico di sviluppo (profilo compose "dev").

Genera post in un formato SEMPLIFICATO, diverso da quello di Jetstream, e li invia via TCP
alla pipeline Logstash "ingest-synthetic" (porta 5001), che li porta allo stesso formato dei
post Bluesky e li scrive su posts.raw. Da li' in poi Q1, Q2 e Q3 non vedono differenze.

Formato di uscita (una riga JSON per post):
  {
    "id": "s1a2b3...",                  identificativo del post
    "author": "user01234",              nome utente
    "created_at": 1794132000123,        epoch in MILLISECONDI (Bluesky: stringa ISO 8601)
    "language": "en",                   stringa, anche "EN" o "en,es" (Bluesky: array langs)
    "content": "...",                   testo
    "reply_to": "user00042/s9f8e7...",  opzionale: "autore/id" del post a cui risponde
    "quote_of": "user00077/s1c2d3...",  opzionale: "autore/id" del post citato
    "link": {"url": "...", "title": "..."},  opzionale
    "phase": "baseline"                 fase dello scenario (verita' di riferimento)
  }

Tutti i parametri dello scenario stanno in config/scenario.toml, riletto a caldo ogni 2 s:
si salva il file e la modifica vale subito, senza riavviare il container.
"""
import copy
import json
import math
import os
import random
import socket
import sys
import time
import tomllib
from collections import deque
from datetime import datetime, timezone

import textgen

LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "logstash")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", "5001"))
SCENARIO_FILE = os.getenv("SCENARIO_FILE", "/config/scenario.toml")

TICK_SECONDS = 0.2          # ogni quanto (tempo reale) si generano e inviano i post maturati
RELOAD_SECONDS = 2.0        # ogni quanto si controlla se scenario.toml e' cambiato
STATS_SECONDS = 30.0        # ogni quanto si stampa una riga di statistiche

# Soglie del detector (q3_detect.py, valori di default): servono solo per gli avvisi all'avvio
DET_MIN_POSTS = 300
DET_R_MIN = 0.8
# Capacita' di Q1 con i valori del .env: 1500 post ogni 10 s
Q1_MAX_POSTS_PER_SECOND = 150

# Valori usati se una chiave manca in scenario.toml (stessi significati, vedi il file)
DEFAULTS = {
    "clock": {"seed": 42, "backfill_minutes": 0.0, "speedup": 1.0},
    "traffic": {"rate_per_minute": 600.0, "jitter_seconds": 3.0, "paused": False},
    "authors": {"pool_size": 8000, "spam_share": 0.0, "spam_accounts": 5},
    "sentiment": {"positive": 0.35, "negative": 0.25, "neutral": 0.40,
                  "drift_amplitude": 0.0, "drift_period_minutes": 240.0},
    "shock": {"mode": "periodic", "first_after_minutes": 60.0, "every_minutes": 90.0,
              "duration_minutes": 6.0, "ramp_minutes": 1.0, "direction": "negative",
              "intensity": 0.30, "volume_factor": 1.0, "topic": "the payment outage",
              "topic_share": 0.7, "active": False},
    "content": {"reply_rate": 0.15, "quote_rate": 0.05, "link_rate": 0.10, "mention_rate": 0.10,
                "url_in_text_rate": 0.05, "language_variant_rate": 0.03},
    "faults": {"duplicate_rate": 0.01, "non_english_rate": 0.03, "empty_text_rate": 0.01,
               "late_rate": 0.0, "late_seconds_min": 150.0, "late_seconds_max": 300.0,
               "future_rate": 0.0, "future_minutes_min": 15.0, "future_minutes_max": 60.0,
               "malformed_json_rate": 0.0, "invalid_rate": 0.0},
}


def log(msg: str) -> None:
    print(msg, flush=True)


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------
def merge_defaults(raw: dict) -> dict:
    cfg = copy.deepcopy(DEFAULTS)
    for section, values in raw.items():
        if section in cfg and isinstance(values, dict):
            for key, value in values.items():
                if key in cfg[section]:
                    cfg[section][key] = value
                else:
                    log(f"[config] chiave sconosciuta ignorata: [{section}] {key}")
        else:
            log(f"[config] sezione sconosciuta ignorata: [{section}]")
    return cfg


def validate(cfg: dict) -> dict:
    def clamp01(section, key):
        cfg[section][key] = min(1.0, max(0.0, float(cfg[section][key])))

    for key in DEFAULTS["content"]:
        clamp01("content", key)
    for key in ("duplicate_rate", "non_english_rate", "empty_text_rate", "late_rate",
                "future_rate", "malformed_json_rate", "invalid_rate"):
        clamp01("faults", key)
    for key in ("spam_share",):
        clamp01("authors", key)
    for key in ("intensity", "topic_share"):
        clamp01("shock", key)

    s = cfg["sentiment"]
    total = sum(max(0.0, float(s[k])) for k in ("positive", "negative", "neutral"))
    if total <= 0:
        raise ValueError("[sentiment]: positive + negative + neutral deve essere > 0")
    sh = cfg["shock"]
    if sh["mode"] not in ("off", "periodic", "manual"):
        raise ValueError(f"[shock] mode = {sh['mode']!r}: valori off, periodic, manual")
    if sh["direction"] not in ("negative", "positive", "alternate"):
        raise ValueError(f"[shock] direction = {sh['direction']!r}: valori negative, positive, alternate")
    if float(sh["every_minutes"]) <= 0 or float(sh["duration_minutes"]) <= 0:
        raise ValueError("[shock] every_minutes e duration_minutes devono essere > 0")
    sh["duration_minutes"] = min(float(sh["duration_minutes"]), float(sh["every_minutes"]))
    if float(sh["volume_factor"]) < 0:
        raise ValueError("[shock] volume_factor deve essere >= 0")
    if int(cfg["authors"]["pool_size"]) < 1 or int(cfg["authors"]["spam_accounts"]) < 1:
        raise ValueError("[authors] pool_size e spam_accounts devono essere >= 1")
    if float(cfg["clock"]["speedup"]) < 1:
        raise ValueError("[clock] speedup deve essere >= 1")
    f = cfg["faults"]
    if f["late_seconds_min"] > f["late_seconds_max"] or f["future_minutes_min"] > f["future_minutes_max"]:
        raise ValueError("[faults]: i minimi non possono superare i massimi")
    return cfg


class ConfigLoader:
    """Rilegge scenario.toml quando cambia. Un file sbagliato non ferma il producer:
    si tiene la configurazione precedente e si stampa l'errore."""

    def __init__(self, path: str):
        self.path = path
        self.mtime = None
        self.cfg = None
        self.last_check = 0.0
        if not self.reload():
            sys.exit(f"[config] {path}: configurazione iniziale non valida")

    def reload(self) -> bool:
        try:
            mtime = os.stat(self.path).st_mtime
            with open(self.path, "rb") as fh:
                cfg = validate(merge_defaults(tomllib.load(fh)))
        except FileNotFoundError:
            if self.cfg is None:
                log(f"[config] {self.path} non trovato: uso i valori di default")
                self.cfg = validate(copy.deepcopy(DEFAULTS))
                return True
            return False
        except (tomllib.TOMLDecodeError, ValueError, TypeError) as exc:
            log(f"[config] ERRORE in {self.path}: {exc} -> tengo la configurazione precedente")
            self.mtime = os.stat(self.path).st_mtime if os.path.exists(self.path) else None
            return False
        first = self.cfg is None
        self.cfg, self.mtime = cfg, mtime
        if not first:
            log(f"[config] scenario ricaricato ({self.path})")
        return True

    def maybe_reload(self) -> None:
        now = time.monotonic()
        if now - self.last_check < RELOAD_SECONDS:
            return
        self.last_check = now
        try:
            mtime = os.stat(self.path).st_mtime
        except FileNotFoundError:
            return
        if mtime != self.mtime:
            old_clock = self.cfg["clock"]
            if self.reload() and self.cfg["clock"] != old_clock:
                log("[config] [clock] si legge solo all'avvio: riavvia il container per applicarlo")


# ---------------------------------------------------------------------------
# Orologio simulato
# ---------------------------------------------------------------------------
class SimClock:
    """Tempo degli eventi (created_at).

    Con backfill_minutes > 0 si parte nel passato e il tempo scorre speedup volte piu' veloce
    finche' non raggiunge l'ora reale; da li' in poi coincide con l'ora reale. Il tempo
    simulato non supera mai l'ora reale: Q2 scarta i post oltre 10 minuti nel futuro.
    """

    def __init__(self, backfill_minutes: float, speedup: float):
        self.real0 = time.time()
        self.sim0 = self.real0 - backfill_minutes * 60
        self.speedup = speedup

    def now(self) -> float:
        real = time.time()
        return min(self.sim0 + (real - self.real0) * self.speedup, real)

    def elapsed_minutes(self, sim_now: float) -> float:
        return (sim_now - self.sim0) / 60


# ---------------------------------------------------------------------------
# Scenario: shock e miscela di sentiment
# ---------------------------------------------------------------------------
def shock_state(cfg: dict, elapsed_min: float):
    """Restituisce (direzione, forza 0..1, numero dello shock)."""
    sh = cfg["shock"]
    direction = "negative" if sh["direction"] == "alternate" else sh["direction"]
    if sh["mode"] == "manual":
        return direction, (1.0 if sh["active"] else 0.0), 0
    if sh["mode"] != "periodic":
        return direction, 0.0, 0
    t = elapsed_min - float(sh["first_after_minutes"])
    if t < 0:
        return direction, 0.0, 0
    every, duration = float(sh["every_minutes"]), float(sh["duration_minutes"])
    idx, k = divmod(t, every)
    idx = int(idx) + 1
    if k >= duration:
        return direction, 0.0, idx
    if sh["direction"] == "alternate":
        direction = "negative" if idx % 2 == 1 else "positive"
    ramp = min(float(sh["ramp_minutes"]), duration / 2)
    strength = 1.0 if ramp <= 0 else min(1.0, k / ramp, (duration - k) / ramp)
    # Anche il primo istante dello shock deve avere forza > 0
    return direction, max(strength, 1e-3), idx


def sentiment_mix(cfg: dict, elapsed_min: float, direction: str, strength: float):
    s = cfg["sentiment"]
    pos, neg, neu = (max(0.0, float(s[k])) for k in ("positive", "negative", "neutral"))
    total = pos + neg + neu
    pos, neg, neu = pos / total, neg / total, neu / total

    # Deriva lenta: sposta quota tra positivo e negativo con una sinusoide. L'EWMA del detector
    # deve assorbirla senza aprire episodi.
    amp, period = float(s["drift_amplitude"]), float(s["drift_period_minutes"])
    if amp > 0 and period > 0:
        delta = amp * math.sin(2 * math.pi * elapsed_min / period)
        pos, neg = max(0.0, pos + delta), max(0.0, neg - delta)
        total = pos + neg + neu
        pos, neg, neu = pos / total, neg / total, neu / total

    # Shock: una quota `intensity` dei post non ancora nella direzione dello shock passa a quella
    intensity = float(cfg["shock"]["intensity"]) * strength
    if intensity > 0:
        if direction == "negative":
            neg += intensity * (1 - neg)
            pos *= 1 - intensity
            neu *= 1 - intensity
        else:
            pos += intensity * (1 - pos)
            neg *= 1 - intensity
            neu *= 1 - intensity
    return pos, neg, neu


def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


# ---------------------------------------------------------------------------
# Generazione degli eventi
# ---------------------------------------------------------------------------
class Generator:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.recent = deque(maxlen=2000)       # (autore, id, riga JSON): risposte, citazioni, duplicati
        self.counter = 0

    def new_id(self) -> str:
        self.counter += 1
        return f"s{self.rng.getrandbits(48):012x}{self.counter:x}"

    def author(self, cfg: dict):
        a = cfg["authors"]
        if self.rng.random() < a["spam_share"]:
            return f"bot{self.rng.randrange(int(a['spam_accounts'])):03d}", True
        return f"user{self.rng.randrange(int(a['pool_size'])):05d}", False

    def one(self, cfg: dict, created: float, real_now: float, mix, shock_dir: str,
            shock_on: bool, phase: str) -> str:
        rng, f, c = self.rng, cfg["faults"], cfg["content"]

        # Duplicato: la stessa riga gia' inviata (stesso id) -> deve sparire nella deduplica
        if self.recent and rng.random() < f["duplicate_rate"]:
            return rng.choice(self.recent)[2]

        # JSON rotto -> posts.dlq con motivo json_non_valido
        if rng.random() < f["malformed_json_rate"]:
            return '{"id": "' + self.new_id() + '", "author": "user1", "content": "troncato'

        # Event time: normale (con un piccolo disordine), in ritardo o nel futuro
        if rng.random() < f["future_rate"]:
            created = real_now + rng.uniform(f["future_minutes_min"], f["future_minutes_max"]) * 60
        elif rng.random() < f["late_rate"]:
            created -= rng.uniform(f["late_seconds_min"], f["late_seconds_max"])
        else:
            created -= rng.uniform(0, float(cfg["traffic"]["jitter_seconds"]))

        author, is_spam = self.author(cfg)
        ev = {"id": self.new_id(), "author": author, "created_at": int(created * 1000), "language": "en"}

        if is_spam:
            ev["content"] = textgen.spam_text(rng)
        elif rng.random() < f["non_english_rate"]:
            ev["content"], ev["language"] = textgen.foreign_text(rng)
        elif rng.random() < f["empty_text_rate"]:
            ev["content"] = rng.choice(["", "   "])
        else:
            pos, neg, _ = mix
            r = rng.random()
            label = "positive" if r < pos else "negative" if r < pos + neg else "neutral"
            topic = textgen.random_topic(rng)
            if shock_on and label == shock_dir and rng.random() < cfg["shock"]["topic_share"]:
                topic = cfg["shock"]["topic"]
            ev["content"] = textgen.post_text(rng, label, topic, c)

        # Varianti del campo lingua: Logstash le normalizza (minuscolo, split su virgola)
        if ev["language"] == "en" and rng.random() < c["language_variant_rate"]:
            ev["language"] = rng.choice(["EN", "en,es", "En, it"])

        if self.recent and rng.random() < c["reply_rate"]:
            a, i, _ = rng.choice(self.recent)
            ev["reply_to"] = f"{a}/{i}"
        if self.recent and rng.random() < c["quote_rate"]:
            a, i, _ = rng.choice(self.recent)
            ev["quote_of"] = f"{a}/{i}"
        if rng.random() < c["link_rate"]:
            ev["link"] = textgen.link_preview(rng)
        ev["phase"] = phase

        # Campo obbligatorio rotto -> posts.dlq con il motivo corrispondente
        if rng.random() < f["invalid_rate"]:
            kind = rng.choice(["author", "id", "created_at", "content_missing", "content_long"])
            if kind == "author":
                ev["author"] = "Not A Valid Name!"
            elif kind == "id":
                ev["id"] = "id con spazi"
            elif kind == "created_at":
                ev["created_at"] = "yesterday"
            elif kind == "content_missing":
                del ev["content"]
            else:
                ev["content"] = "x" * 3500
            return json.dumps(ev, ensure_ascii=False)

        line = json.dumps(ev, ensure_ascii=False)
        self.recent.append((ev["author"], ev["id"], line))
        return line


# ---------------------------------------------------------------------------
# Invio a Logstash
# ---------------------------------------------------------------------------
class Sender:
    def __init__(self, host: str, port: int):
        self.host, self.port, self.sock = host, port, None

    def connect(self) -> None:
        delay = 1.0
        while self.sock is None:
            try:
                self.sock = socket.create_connection((self.host, self.port), timeout=10)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                log(f"[tcp] connesso a {self.host}:{self.port}")
            except OSError as exc:
                # All'avvio la pipeline ingest-synthetic puo' partire qualche secondo dopo la 5000
                log(f"[tcp] {self.host}:{self.port} non raggiungibile ({exc}), riprovo tra {delay:.0f}s")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)

    def send(self, lines) -> None:
        if not lines:
            return
        data = ("\n".join(lines) + "\n").encode("utf-8")
        for _ in range(2):
            if self.sock is None:
                self.connect()
            try:
                self.sock.sendall(data)
                return
            except OSError as exc:
                log(f"[tcp] invio fallito ({exc}): riconnessione")
                try:
                    self.sock.close()
                finally:
                    self.sock = None
        log(f"[tcp] {len(lines)} post persi")


# ---------------------------------------------------------------------------
def startup_checks(cfg: dict) -> None:
    rate = float(cfg["traffic"]["rate_per_minute"])
    pool = int(cfg["authors"]["pool_size"])
    speedup = float(cfg["clock"]["speedup"])
    if rate < DET_MIN_POSTS * 1.2:
        log(f"[avviso] rate_per_minute={rate:.0f}: vicino o sotto DET_MIN_POSTS={DET_MIN_POSTS}, "
            f"molte finestre saranno low_volume")
    # Autori distinti attesi in una finestra di un minuto con autori estratti uniformemente
    ratio = pool * (1 - math.exp(-rate / pool)) / rate if rate > 0 else 1.0
    if ratio < DET_R_MIN + 0.05:
        log(f"[avviso] pool_size={pool}: rapporto autori/post atteso {ratio:.2f}, "
            f"vicino o sotto DET_R_MIN={DET_R_MIN} (finestre low_diversity)")
    real_rate = rate * speedup / 60
    if real_rate > Q1_MAX_POSTS_PER_SECOND * 0.8:
        log(f"[avviso] {real_rate:.0f} post/s reali: vicino alla capacita' di Q1 "
            f"(~{Q1_MAX_POSTS_PER_SECOND}/s), l'arretrato su posts.raw crescera'")


def main() -> None:
    loader = ConfigLoader(SCENARIO_FILE)
    cfg = loader.cfg
    clock = SimClock(float(cfg["clock"]["backfill_minutes"]), float(cfg["clock"]["speedup"]))
    rng = random.Random(cfg["clock"]["seed"])
    gen = Generator(rng)
    sender = Sender(LOGSTASH_HOST, LOGSTASH_PORT)

    log(f"[avvio] scenario {SCENARIO_FILE}, Logstash {LOGSTASH_HOST}:{LOGSTASH_PORT}")
    log(f"[avvio] tempo simulato da {iso(clock.sim0)}, speedup x{clock.speedup:g}")
    startup_checks(cfg)
    sender.connect()

    last_sim = clock.now()
    last_stats = time.monotonic()
    sent_since_stats, sim_since_stats = 0, 0.0
    shock_was_on, caught_up = False, clock.sim0 >= clock.real0

    while True:
        time.sleep(TICK_SECONDS)
        loader.maybe_reload()
        cfg = loader.cfg

        real_now = time.time()
        sim_now = clock.now()
        dt = sim_now - last_sim
        elapsed = clock.elapsed_minutes(sim_now)

        direction, strength, idx = shock_state(cfg, elapsed)
        shock_on = strength > 0
        if shock_on != shock_was_on:
            # Verita' di riferimento: da confrontare con gli alert in Kibana (alert_id = shock-<finestra>)
            what = "INIZIO" if shock_on else "FINE"
            log(f"[shock] {what} #{idx} direzione={direction} tempo evento={iso(sim_now)}")
            shock_was_on = shock_on
        if not caught_up and sim_now >= real_now - 1:
            caught_up = True
            log(f"[clock] backfill completato: da ora tempo reale ({iso(sim_now)})")

        lines = []
        if not cfg["traffic"]["paused"] and dt > 0:
            volume = float(cfg["shock"]["volume_factor"]) if shock_on else 1.0
            lam = float(cfg["traffic"]["rate_per_minute"]) / 60 * dt * volume
            mix = sentiment_mix(cfg, elapsed, direction, strength)
            phase = f"shock-{direction}" if shock_on else "baseline"
            for _ in range(poisson(rng, lam)):
                created = rng.uniform(last_sim, sim_now)
                lines.append(gen.one(cfg, created, real_now, mix, direction, shock_on, phase))
        last_sim = sim_now
        sender.send(lines)

        sent_since_stats += len(lines)
        sim_since_stats += dt
        if time.monotonic() - last_stats >= STATS_SECONDS:
            pos, neg, neu = sentiment_mix(cfg, elapsed, direction, strength)
            per_min = sent_since_stats / (sim_since_stats / 60) if sim_since_stats > 0 else 0.0
            state = "IN PAUSA" if cfg["traffic"]["paused"] else (
                f"shock {direction} {strength:.0%}" if shock_on else "baseline")
            log(f"[stats] tempo evento {iso(sim_now)} | {sent_since_stats} righe, "
                f"{per_min:.0f}/min simulato | {state} | pos={pos:.2f} neg={neg:.2f} neu={neu:.2f}")
            last_stats, sent_since_stats, sim_since_stats = time.monotonic(), 0, 0.0


if __name__ == "__main__":
    main()