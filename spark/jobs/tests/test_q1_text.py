import os
import sys

# Rende importabile q1_enrich.py, che sta nella cartella superiore
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from q1_enrich import (GAMING_TERMS_PATH, add_scope, add_terms, build_text_pipeline,
                       content_col, load_dictionary, parse_posts)

# --- Parsing: messaggi grezzi come arrivano da Kafka (colonna value).
# (descrizione, value, uri, deve_restare, link_title atteso se resta)
T = "2026-10-27T15:43:03.562013Z"
PARSE_CASES = [
    ("valido",                 f'{{"uri":"at://ok","did":"d","time":"{T}","text":"hi","link_title":"Nintendo Direct"}}', "at://ok", True, "Nintendo Direct"),
    ("titolo vuoto -> null",   f'{{"uri":"at://empty","did":"d","time":"{T}","text":"hi","link_title":""}}',             "at://empty", True, None),
    ("titolo di spazi -> null",f'{{"uri":"at://blank","did":"d","time":"{T}","text":"hi","link_title":"   "}}',          "at://blank", True, None),
    ("time non convertibile",  '{"uri":"at://badtime","did":"d","time":"ieri sera","text":"hi"}',                        "at://badtime", False, None),
    ("time mancante",          '{"uri":"at://notime","did":"d","text":"hi"}',                                            "at://notime", False, None),
    ("JSON rotto",             '{"uri":"at://broken","did":',                                                            "at://broken", False, None),
]

# --- Termini: testo, termini che DEVONO esserci, termini che NON devono esserci.
# "empty": True -> il post non deve produrre alcun termine.
TERM_CASES = [
    {"text": "Nintendo's new Switch looks great", "must": ["nintendo", "switch"], "must_not": ["nintendo's"]},
    {"text": "Pokémon Legends is out",            "must": ["pokemon"],            "must_not": ["pok", "mon"]},
    {"text": "Check kick.com/abc and https://x.com/y now", "must": ["check"],     "must_not": ["kick", "com", "abc", "https"]},
    {"text": "@ucsandiego.bsky.social great school", "must": ["school"],          "must_not": ["bsky", "social", "ucsandiego"]},
    {"text": "Playing #X-Box tonight",            "must": ["xbox"]},
    {"text": "I don’t like it",                   "must_not": ["don't", "don’t"]},
    {"text": "O'Sullivan wins again",             "must": ["o'sullivan"]},
    {"text": "Elden Ring DLC on PS5 and Nintendo 3DS", "must": ["elden ring", "ps5", "3ds"], "must_not": ["ds"]},
    {"text": "Trails in the Sky 2nd Chapter",     "must_not": ["nd"]},
    {"text": "NBA 2K25 in 4K",                    "must": ["2k25", "4k"],         "must_not": ["k25"]},
    # Documenta una scelta, non un errore: i bigrammi si formano DOPO le stopword
    {"text": "Call of Duty is back",              "must": ["call duty"]},
    {"text": "😂😂😂",                             "empty": True},
]

# --- Ambito: testo, titolo del link (o None), esito atteso
SCOPE_CASES = [
    # Deboli dello stesso tema non-gaming: devono restare FUORI
    ("Congress remains in deadlock over who will control the budget", None, False),
    ("Russian troops fired an RPG across the battlefield, the warzone keeps expanding", None, False),
    ("Nuclear fallout fears grow as the doom scenario looms", None, False),
    ("FIFA confirms the marathon schedule for the World Cup opening", None, False),
    ("Luigi Mangione trial: Mario Cuomo-style defense from Bowser?", None, False),
    ("Shipped the patch today, now clearing my backlog", None, False),
    ("My naughty dog chewed my shoes again", None, False),
    # Gaming vero: deve entrare
    ("sov will drop the worst fire emblem map you have played", None, True),
    ("The new Hades patch nerfed my build", None, True),
    ("Doom on my old console still slaps", None, True),
    ("New DLC and a season pass announced", None, True),
    ("Naughty Dog just posted a new job listing for multiplayer designers", None, True),
    ("Counter Strike major this weekend", None, True),       # variante con spazio di Counter-Strike
    # link_title: il testo da solo non basta, il titolo si'
    ("finally!", "Nintendo Direct announced for Thursday", True),
]

spark = SparkSession.builder.appName("test_q1_text").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
pipeline = build_text_pipeline()
failures = 0

# ---------------------------------------------------------------- parsing
# Gira con la configurazione predefinita di Spark 4 (ANSI attivo), la stessa del job
print("== Parsing")
raw = spark.createDataFrame([(c[1],) for c in PARSE_CASES], "value STRING")
kept = {r["uri"]: r for r in parse_posts(raw).select("uri", "link_title", "event_time").collect()}

for desc, _, uri, should_stay, title in PARSE_CASES:
    if should_stay:
        ok = uri in kept and kept[uri]["link_title"] == title
    else:
        ok = uri not in kept
    failures += not ok
    print(f"{'OK' if ok else 'KO'}  {desc}")
    if not ok:
        print(f"      in uscita: {kept.get(uri)}")

# ---------------------------------------------------------------- termini
print("\n== Termini")
df = spark.createDataFrame([(c["text"],) for c in TERM_CASES], "text STRING")
# Indicizzo per testo: in un sistema distribuito l'ordine delle righe non e' garantito
results = {r["text"]: set(r["terms"]) for r in add_terms(df, pipeline).select("text", "terms").collect()}

for case in TERM_CASES:
    terms = results[case["text"]]
    missing = [t for t in case.get("must", []) if t not in terms]
    present = [t for t in case.get("must_not", []) if t in terms]
    not_empty = case.get("empty", False) and len(terms) > 0
    ok = not missing and not present and not not_empty
    failures += not ok
    print(f"{'OK' if ok else 'KO'}  {case['text']!r}")
    if not ok:
        print(f"      termini: {sorted(terms)}")
        print(f"      mancano: {missing}   non dovevano esserci: {present}   vuoto atteso: {bool(not_empty)}")

# ---------------------------------------------------------------- ambito
print("\n== Ambito")
dictionary = load_dictionary(spark, pipeline, GAMING_TERMS_PATH)
df = spark.createDataFrame(SCOPE_CASES, "text STRING, link_title STRING, expected BOOLEAN")
scoped = add_scope(add_terms(df.withColumn("content", content_col()), pipeline, text_col="content"), dictionary)

for r in scoped.select("text", "expected", "in_scope", "scope_terms").collect():
    ok = r["in_scope"] == r["expected"]
    failures += not ok
    print(f"{'OK' if ok else 'KO'}  atteso {'IN ' if r['expected'] else 'OUT'}  {r['text']!r}")
    if not ok:
        print(f"      scope_terms: {r['scope_terms']}")

total = len(PARSE_CASES) + len(TERM_CASES) + len(SCOPE_CASES)
print(f"\n{total - failures}/{total} casi superati")
spark.stop()
sys.exit(1 if failures else 0)