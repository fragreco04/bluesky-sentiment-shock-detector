"""
Benchmark del modello di sentiment, fuori da Spark, sui post veri di posts.raw.

Misura:
  1. lunghezza dei testi in token: quanto lavoro fa il modello per ogni post;
  2. throughput (post/s) per ogni combinazione processi x thread e per ogni motore
     (PyTorch fp32 e int8, OpenVINO fp32 e int8, su CPU o GPU).
     E' lo stesso schema di Q1: ogni processo ha la sua copia del modello e usa
     sentiment_model.score(), cioe' lo stesso codice della pipeline;
  3. quanto ogni motore si discosta da PyTorch fp32 (il riferimento): etichette,
     score, media del campione.

Non misura l'overhead di Spark (Kafka, deduplica, serializzazione Arrow): la
capacita' reale di Q1 sara' un po' piu' bassa del throughput trovato qui.

Uso (dentro l'immagine Spark, con Q1 fermo):
  python3 bench_sentiment.py sample.jsonl --n 2000
  python3 bench_sentiment.py sample.jsonl --configs 2x2 --engines torch,ov:fp32,ov:int8,ov:int8-nosq
  python3 bench_sentiment.py sample.jsonl --configs 1x4 --engines torch,ov:fp32@GPU,ov:int8@GPU

I motori OpenVINO usano i modelli esportati da ov_export.py in SENTIMENT_OV_DIR.
"""
import argparse
import json
import multiprocessing as mp
import os
import resource
import sys
import time

# Questo file sta accanto a sentiment_model.py (spark/jobs): stesso codice della pipeline.
# Niente import di torch qui: lo importa solo ogni processo figlio, dopo aver fissato i thread.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def read_texts(path, n):
    # Una riga = un messaggio JSON di posts.raw (output di kafka-console-consumer)
    texts = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            t = obj.get("text")
            if isinstance(t, str) and t.strip():
                texts.append(t)
            if len(texts) >= n:
                break
    return texts


def token_stats(texts, model_path, max_length):
    from transformers import AutoTokenizer
    import sentiment_model

    tok = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    lengths = sorted(len(ids) for ids in tok([sentiment_model.preprocess(t) for t in texts])["input_ids"])
    n = len(lengths)
    pct = lambda p: lengths[min(n - 1, int(p / 100 * n))]
    over = sum(1 for x in lengths if x > max_length)
    print(f"Token per post (dopo il preprocessing, con <s> e </s>):")
    print(f"  media {sum(lengths) / n:.1f}   mediana {pct(50)}   p95 {pct(95)}   p99 {pct(99)}   max {lengths[-1]}")
    print(f"  oltre max_length={max_length}: {over} post ({100 * over / n:.2f}%), vengono troncati")
    print()


def _worker(rank, shard, model_path, threads, engine, ov_dir, batch_size, max_length, barrier, out_q):
    # Prima di importare torch, come fa Q1 con spark.executorEnv
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        import sentiment_model

        sentiment_model.load(model_path, threads, engine, ov_dir)
        # Riscaldamento: la prima passata alloca buffer e prepara i kernel, non va cronometrata.
        # Sulla GPU compila le forme piu' frequenti (la cache su disco accelera le volte successive).
        sentiment_model.score(shard[: 4 * batch_size], model_path, batch_size, max_length, threads, engine, ov_dir)
        barrier.wait()  # tutti i processi partono insieme, come i task paralleli di Q1
        t0 = time.perf_counter()
        scores, labels = sentiment_model.score(shard, model_path, batch_size, max_length, threads, engine, ov_dir)
        elapsed = time.perf_counter() - t0
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KB
        out_q.put((rank, elapsed, rss_mb, scores, labels, None))
    except Exception as e:  # il padre deve ricevere comunque un risultato, o resta bloccato
        try:
            barrier.abort()
        except Exception:
            pass
        out_q.put((rank, 0.0, 0.0, [], [], repr(e)))


def run_config(texts, model_path, procs, threads, engine, ov_dir, batch_size, max_length):
    ctx = mp.get_context("spawn")  # processi nuovi: niente stato di torch ereditato dal padre
    barrier = ctx.Barrier(procs)
    out_q = ctx.Queue()
    # Shard interlacciati: ogni processo riceve testi di lunghezza simile agli altri
    shards = [texts[r::procs] for r in range(procs)]
    ps = [
        ctx.Process(target=_worker,
                    args=(r, shards[r], model_path, threads, engine, ov_dir, batch_size, max_length, barrier, out_q))
        for r in range(procs)
    ]
    for p in ps:
        p.start()
    # Prima si svuota la coda, poi join (altrimenti deadlock). Se un processo muore senza
    # rispondere (es. ucciso per memoria esaurita) non si resta ad aspettarlo per sempre.
    import queue
    results = []
    while len(results) < procs:
        try:
            results.append(out_q.get(timeout=5))
        except queue.Empty:
            dead = [p for p in ps if p.exitcode not in (None, 0)]
            if dead:
                for p in ps:
                    p.terminate()
                raise RuntimeError(f"processo terminato con codice {dead[0].exitcode} "
                                   "(-9 = ucciso, di solito per memoria esaurita)")
    for p in ps:
        p.join()

    errors = [r[5] for r in results if r[5]]
    if errors:
        raise RuntimeError(errors[0])

    scores = [0.0] * len(texts)
    labels = [""] * len(texts)
    for rank, _, _, s, l, _ in results:
        scores[rank::procs] = s
        labels[rank::procs] = l
    wall = max(r[1] for r in results)
    rss = max(r[2] for r in results)
    return len(texts) / wall, rss, scores, labels


def compare(ref_scores, ref_labels, scores, labels):
    import numpy as np

    a, b = np.asarray(ref_scores), np.asarray(scores)
    d = np.abs(a - b)
    n = len(ref_labels)
    same = sum(x == y for x, y in zip(ref_labels, labels))
    flips = sum({x, y} == {"negative", "positive"} for x, y in zip(ref_labels, labels))
    # Spostamento medio separato per classe (secondo il riferimento): se il motore sposta
    # i post negativi diversamente dai positivi, comprime o amplifica gli shock.
    ref_lab = np.asarray(ref_labels)
    by_class = {}
    for cls, short in (("negative", "neg"), ("neutral", "neu"), ("positive", "pos")):
        sel = ref_lab == cls
        by_class[f"spost. {short}"] = f"{(b[sel] - a[sel]).mean():+.4f}" if sel.any() else "-"
    return {
        "etich. uguali": f"{100 * same / n:.1f}%",
        "neg<->pos": f"{100 * flips / n:.2f}%",
        "|diff| medio": f"{d.mean():.4f}",
        "|diff| p99": f"{np.percentile(d, 99):.4f}",
        "correlaz.": f"{np.corrcoef(a, b)[0, 1]:.4f}",
        "spost. media": f"{b.mean() - a.mean():+.4f}",
        **by_class,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample", help="file JSONL con i messaggi di posts.raw")
    ap.add_argument("--n", type=int, default=2000, help="post da usare (default 2000)")
    ap.add_argument("--configs", default="1x4,2x2,4x1", help="processi x thread, separati da virgola")
    ap.add_argument("--batch-sizes", default="32")
    ap.add_argument("--engines", "--modes", dest="engines", default="torch,ov:fp32,ov:int8",
                    help="motori separati da virgola; torch (PyTorch fp32) e' sempre il riferimento")
    ap.add_argument("--max-length", type=int, default=int(os.getenv("INFER_MAX_LENGTH", "256")))
    ap.add_argument("--model-path", default=os.getenv("SENTIMENT_MODEL_PATH"))
    ap.add_argument("--ov-dir", default=os.getenv("SENTIMENT_OV_DIR"))
    args = ap.parse_args()

    if not args.model_path:
        sys.exit("SENTIMENT_MODEL_PATH non impostato: passa --model-path")
    texts = read_texts(args.sample, args.n)
    if len(texts) < 200:
        sys.exit(f"Solo {len(texts)} post con testo in {args.sample}: ne servono almeno 200")
    print(f"{len(texts)} post da {args.sample}, modello {args.model_path}\n")

    token_stats(texts, args.model_path, args.max_length)

    configs = [tuple(int(x) for x in c.lower().split("x")) for c in args.configs.split(",")]
    batch_sizes = [int(b) for b in args.batch_sizes.split(",")]
    import sentiment_model  # solo per validare i nomi: all'import non carica torch
    # Forma canonica + controllo che i modelli OpenVINO esistano, prima di perdere tempo
    engines = [sentiment_model.check_engine(e, args.ov_dir) for e in args.engines.split(",") if e.strip()]
    if "torch" not in engines:
        engines.insert(0, "torch")  # il riferimento serve sempre per il confronto

    rows, outputs = [], {}
    for procs, threads in configs:
        for bs in batch_sizes:
            for mode in engines:
                name = f"{procs}x{threads} bs={bs} {mode}"
                print(f"  {name} ...", end="", flush=True)
                try:
                    tput, rss, s, l = run_config(texts, args.model_path, procs, threads,
                                                 mode, args.ov_dir, bs, args.max_length)
                except RuntimeError as e:
                    print(f" fallita: {e}")
                    continue
                print(f" {tput:6.1f} post/s")
                rows.append((name, tput, rss, procs))
                outputs[(procs, threads, bs, mode)] = (s, l)

    print("\nThroughput (stesso campione per tutte le configurazioni)")
    # Picco di RSS: include il momento del caricamento, prima che la memoria si stabilizzi
    print(f"  {'configurazione':<30}{'post/s':>8}{'RAM picco/processo':>20}{'RAM totale':>12}")
    for name, tput, rss, procs in sorted(rows, key=lambda r: -r[1]):
        print(f"  {name:<30}{tput:8.1f}{rss / 1024:18.2f} GB{rss * procs / 1024:10.2f} GB")

    # Fedelta': ogni motore contro PyTorch fp32 della stessa configurazione (stessi post, stessi batch)
    pairs = [((p, t, b, "torch"), k) for (p, t, b, m) in outputs for k in [(p, t, b, m)]
             if m != "torch" and (p, t, b, "torch") in outputs]
    if pairs:
        print(f"\nMotori contro PyTorch fp32 ({len(texts)} post)")
        cols = None
        for ref, var in pairs:
            res = compare(*outputs[ref], *outputs[var])
            if cols is None:
                cols = list(res)
                print(f"  {'motore':<30}" + "".join(f"{c:>14}" for c in cols))
            name = f"{var[0]}x{var[1]} bs={var[2]} {var[3]}"
            print(f"  {name:<30}" + "".join(f"{res[c]:>14}" for c in cols))


if __name__ == "__main__":
    main()