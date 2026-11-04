"""
Benchmark dell'inferenza di sentiment, fuori da Spark, sui post veri.

Simula i task paralleli di Q1: N processi, ognuno con la sua copia del modello e T thread torch,
che si dividono gli stessi testi. Per ogni configurazione misura:
  - post/s complessivi (il tempo di caricamento del modello e' escluso, come in Q1 a regime)
  - memoria massima di un processo
  - differenza rispetto alla prima configurazione (quella attuale): etichette uguali e
    spostamento della media del sentiment, che e' cio' che usa il detector

Uso (dentro l'immagine Spark, con Q1 fermo per non contendersi la CPU):
  python3 bench_sentiment.py bench_posts.jsonl
Lo script puo' stare in jobs/ o in una sua sottocartella (es. jobs/tests/):
sentiment_model.py viene cercato nella cartella dello script e in quella superiore.
Variabili opzionali: BENCH_N (testi, default 1200), SENTIMENT_MODEL_PATH.
"""
import json
import multiprocessing as mp
import os
import queue
import resource
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SEARCH_PATH = [HERE, os.path.dirname(HERE)]     # jobs/tests/ -> anche jobs/

MODEL_PATH = os.environ["SENTIMENT_MODEL_PATH"]
N_TEXTS = int(os.getenv("BENCH_N", "1200"))
BATCH_SIZE = 32

# (processi, thread per processo, int8, max_length). La prima e' la configurazione attuale di Q1.
CONFIGS = [
    (2, 2, False, 256),
    (2, 2, False, 128),
    (2, 2, True, 128),
    (4, 1, True, 128),
    (1, 4, True, 128),
]


def read_texts(path, n):
    # Accetta righe JSON di posts.raw (campo "text") e conta quelle scartate, per capire il perche'
    texts = []
    lines = invalid = no_text = 0
    first_bad = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            lines += 1
            try:
                obj = json.loads(line)
            except ValueError:
                invalid += 1
                first_bad = first_bad or line[:200]
                continue
            t = obj.get("text") if isinstance(obj, dict) else None
            if not t:
                no_text += 1
                first_bad = first_bad or line[:200]
                continue
            texts.append(t)
            if len(texts) >= n:
                break
    if not texts:
        sys.exit(
            f"Nessun testo in {path}: righe {lines}, JSON non validi {invalid}, senza 'text' {no_text}.\n"
            f"Prima riga scartata: {first_bad!r}"
        )
    return texts


def worker(idx, texts, threads, quantize, max_length, barrier, out):
    sys.path[:0] = SEARCH_PATH
    import sentiment_model

    # Caricamento + un passaggio di riscaldamento, fuori dalla misura
    sentiment_model.score(texts[:BATCH_SIZE], MODEL_PATH, BATCH_SIZE, max_length, threads, quantize)
    barrier.wait()                                      # tutti i processi partono insieme
    t0 = time.perf_counter()
    scores, labels = sentiment_model.score(texts, MODEL_PATH, BATCH_SIZE, max_length, threads, quantize)
    t1 = time.perf_counter()
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024   # KB -> MB su Linux
    out.put((idx, t0, t1, scores, labels, rss_mb))


def run(texts, procs, threads, quantize, max_length):
    ctx = mp.get_context("spawn")                       # processi puliti, come i worker di Spark
    chunks = [texts[i::procs] for i in range(procs)]
    barrier, out = ctx.Barrier(procs), ctx.Queue()
    ps = [ctx.Process(target=worker, args=(i, c, threads, quantize, max_length, barrier, out))
          for i, c in enumerate(chunks)]
    for p in ps:
        p.start()
    # Se un processo muore (errore, memoria esaurita) non manda il risultato:
    # invece di aspettare per sempre, ci si ferma e si riporta il codice di uscita.
    results = []
    while len(results) < procs:
        try:
            results.append(out.get(timeout=5))
        except queue.Empty:
            dead = [p for p in ps if p.exitcode not in (None, 0)]
            if dead:
                for p in ps:
                    p.terminate()
                sys.exit(f"Un processo del benchmark e' terminato con codice {dead[0].exitcode} (137 = memoria esaurita)")
    for p in ps:
        p.join()

    results.sort()
    elapsed = max(r[2] for r in results) - min(r[1] for r in results)
    # Ricompone i risultati nell'ordine originale dei testi
    scores, labels = [0.0] * len(texts), [""] * len(texts)
    for idx, _, _, s, l, _ in results:
        positions = range(idx, len(texts), procs)
        for pos, sv, lv in zip(positions, s, l):
            scores[pos], labels[pos] = sv, lv
    return len(texts) / elapsed, max(r[5] for r in results), scores, labels


def main():
    texts = read_texts(sys.argv[1], N_TEXTS)
    print(f"{len(texts)} testi, batch {BATCH_SIZE}, CPU visibili: {os.cpu_count()}\n")
    print(f"{'config':<22}{'post/s':>8}{'x base':>8}{'RAM/proc':>10}{'etich.=':>9}{'d media':>9}")

    base = None
    for procs, threads, quantize, max_length in CONFIGS:
        rate, rss, scores, labels = run(texts, procs, threads, quantize, max_length)
        name = f"{procs}x{threads} {'int8' if quantize else 'fp32'} len{max_length}"
        if base is None:
            base = (rate, scores, labels)
        same = sum(a == b for a, b in zip(labels, base[2])) / len(labels)
        shift = statistics.fmean(scores) - statistics.fmean(base[1])
        print(f"{name:<22}{rate:>8.1f}{rate / base[0]:>8.2f}{rss:>8.0f}MB{same:>9.1%}{shift:>+9.4f}")


if __name__ == "__main__":
    main()