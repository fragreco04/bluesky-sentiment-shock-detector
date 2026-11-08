"""
Testi sintetici per il producer di sviluppo.

Il sentiment NON viene deciso qui: lo calcola Q1 con il modello vero. Qui si scelgono frasi
dal tono inequivocabile (positivo, negativo, neutro), cosi' la quota di post di ciascun tipo
decisa dallo scenario si ritrova, con buona approssimazione, nel sentiment medio delle finestre.

Le decorazioni (menzioni, link con e senza schema) esercitano sentiment_model.preprocess().
"""
import random

TOPICS = [
    "the new album", "today's match", "this update", "the city council vote", "my morning coffee",
    "the weather", "this movie", "the conference", "my new job", "the train service", "the launch",
    "this book", "the concert last night", "the game patch", "our team", "the new phone",
    "the season finale", "the local library", "the marathon", "the food festival", "the new park",
    "the election debate", "the podcast episode", "the museum exhibit", "the airline", "the hospital",
]

LINK_TITLES = [
    "Everything you need to know", "Live updates", "Analysis: what happens next",
    "Photos from the event", "Official statement", "A short guide", "Opinion: a closer look",
]

POSITIVE = [
    "I absolutely love {t}!",
    "{T} is amazing, best thing this week",
    "So happy about {t} 😊",
    "What a wonderful surprise, {t} turned out great",
    "Honestly impressed by {t}. Great work everyone!",
    "{T} made my day ❤️",
    "Can't stop smiling about {t}",
    "Huge congrats on {t}, well deserved 🎉",
    "{T} is fantastic, really proud of this",
    "Grateful for {t}, it was perfect",
]
NEGATIVE = [
    "I hate {t}, total disaster",
    "{T} is awful. So disappointed",
    "Really angry about {t} right now 😡",
    "Worst experience ever with {t}",
    "{T} ruined my whole day",
    "This is terrible news about {t}",
    "Fed up with {t}, absolutely ridiculous",
    "Sad and frustrated about {t} 😢",
    "{T} is a complete mess, unacceptable",
    "Disgusted by {t}. Never again",
]
NEUTRAL = [
    "Reading about {t} this afternoon",
    "{T} is scheduled for Thursday at 3pm",
    "Anyone know when {t} starts?",
    "Notes on {t}: three points to check",
    "Just saw a thread about {t}",
    "{T} will be discussed in the next meeting",
    "Here is a summary of {t}",
    "Looking for more info on {t}",
    "{T} moved to the second floor",
    "Question about {t}: is registration still open?",
]
TEMPLATES = {"positive": POSITIVE, "negative": NEGATIVE, "neutral": NEUTRAL}

PREFIXES = {
    "positive": ["", "", "Wow, ", "Honestly, ", "Finally! "],
    "negative": ["", "", "Ugh, ", "Seriously, ", "Not again. "],
    "neutral": ["", "", "FYI: ", "Update: ", "Quick note: "],
}

# Post non inglesi: Logstash deve scartarli (drop_non_inglese)
FOREIGN = [
    ("Oggi parliamo di {t}, che ne pensate?", "it"),
    ("Che bella giornata, adoro questo posto", "it"),
    ("Hoy hablamos de esto, ¿qué opinan?", "es"),
    ("Qué día tan bonito, me encanta", "es"),
    ("Heute ist ein schöner Tag", "de"),
    ("Je suis très content aujourd'hui", "fr"),
]

SPAM = [
    "Get 1000 followers FAST!!! click now",
    "Win a free phone today, limited offer!!!",
    "Crypto giveaway, send now and double it",
]


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def _slug(rng: random.Random) -> str:
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(8))


def post_text(rng: random.Random, label: str, topic: str, content: dict) -> str:
    template = rng.choice(TEMPLATES[label])
    prefix = rng.choice(PREFIXES[label])
    # Maiuscola solo a inizio frase: "Update: The match..." ma "Honestly, the match..."
    start = prefix == "" or prefix.endswith((". ", "! ", ": "))
    body = template.format(t=topic, T=_cap(topic))
    if not start and not body.startswith(("I ", "I'")):
        body = body[:1].lower() + body[1:]
    text = prefix + body
    if rng.random() < content["mention_rate"]:
        handle = f"@user{rng.randrange(100000)}.bsky.social"
        text = f"{handle} {text}" if rng.random() < 0.5 else f"{text} cc {handle}"
    if rng.random() < content["url_in_text_rate"]:
        url = (f"https://example.com/{_slug(rng)}" if rng.random() < 0.5
               else f"news.example.org/{_slug(rng)}")
        text = f"{text} {url}"
    return text


def random_topic(rng: random.Random) -> str:
    return rng.choice(TOPICS)


def foreign_text(rng: random.Random):
    template, lang = rng.choice(FOREIGN)
    return template.format(t="questo"), lang


def spam_text(rng: random.Random) -> str:
    return f"{rng.choice(SPAM)} https://spam.example.net/{_slug(rng)}"


def link_preview(rng: random.Random) -> dict:
    return {"url": f"https://example.com/{_slug(rng)}", "title": rng.choice(LINK_TITLES)}