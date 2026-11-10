# Appendice A – Topic Kafka e formato dei messaggi

Questa appendice raccoglie i dettagli sul formato dei dati che nella [relazione](Relazione.md) sono riassunti nei capitoli 2, 3 e 6.

## A.1 Un messaggio di Jetstream

Ogni messaggio di Jetstream è un oggetto JSON con due livelli. Il livello esterno descrive l'evento: chi l'ha fatto, quando, che tipo di operazione è e su quale record. Al suo interno, il campo `record` contiene il post vero e proprio. Questo è un esempio semplificato:

```json
{
  "$type": "message",
  "payload": {
    "did": "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
    "time": "2026-10-21T15:08:23.966505Z",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3myfgj6zbwh2o",
    "record": {
      "text": "Just saw the news about the earthquake, hope everyone is safe",
      "createdAt": "2026-10-21T15:08:22.428Z",
      "langs": ["en"],
      "embed": {
        "$type": "app.bsky.embed.external",
        "external": { "uri": "https://...", "title": "Strong earthquake hits..." }
      }
    }
  }
}
```

Il campo `operation` può valere `create` (nuovo post), `update` (post modificato) o `delete` (post cancellato). Al progetto interessano solo i post nuovi, perché il sentiment va misurato nel momento in cui le persone scrivono. Le cancellazioni, tra l'altro, non contengono il testo del post, ma solo il suo indirizzo.

Dentro `record` i campi principali sono:

- `text`, il testo del post, che può anche essere vuoto, per esempio nei post con sola immagine;
- `createdAt`, la data di creazione;
- `langs`, le lingue dichiarate dall'utente o dall'app con cui ha scritto;
- `reply`, presente se il post è una risposta a un altro post;
- `embed`, presente se il post contiene un allegato: immagini, video, l'anteprima di un link o la citazione di un altro post.

## A.2 I campi di posts.raw

| Campo | Presenza | Origine | A cosa serve |
|---|---|---|---|
| `uri` | sempre | costruito da `did` e `rkey` | identifica il post: serve a eliminare i duplicati |
| `did` | sempre | `payload.did` | identifica l'autore: serve a contare gli autori distinti e a riconoscere lo spam |
| `time` | sempre | `payload.time` | stabilisce in quale minuto cade il post |
| `text` | sempre | `record.text` | è l'input del modello di sentiment |
| `langs` | sempre | `record.langs` | conservato solo per tracciare il filtro sulla lingua |
| `link_title` | se presente | titolo dell'anteprima del link | dice di quale notizia si sta parlando |
| `reply_parent_uri` | se presente | `record.reply.parent.uri` | permette di trovare i post più discussi |
| `quoted_uri` | se presente | uri del post citato nell'`embed` | permette di trovare i post più citati |

`reply_parent_uri` contiene l'uri del post a cui si risponde direttamente: se si risponde a un commento, è l'uri di quel commento e non dell'inizio della discussione. `quoted_uri` si omette se l'oggetto citato non è un post (un feed, una lista…). I campi facoltativi compaiono solo se presenti e validi.

## A.3 I controlli di Logstash e la dead letter queue

Sugli eventi che sono commit di post, Logstash controlla che i campi indispensabili siano presenti e abbiano la forma corretta. I controlli sono organizzati in una catena: ogni evento si ferma al primo controllo che non supera, e quel controllo determina il motivo dello scarto.

| Motivo | Quando scatta |
|---|---|
| `json_non_valido` | la riga ricevuta non è un JSON valido |
| `payload_mancante` | il messaggio non contiene l'evento |
| `operation_non_valida` | l'operazione manca o ha un valore sconosciuto |
| `collection_mancante` | manca il tipo di record |
| `did_non_valido` | l'identificativo dell'autore non ha la forma `did:<metodo>:<valore>` |
| `rkey_non_valida` | la chiave del record contiene caratteri non ammessi |
| `time_non_valido` | l'orario non è una data valida |
| `record_mancante` | manca il contenuto del post |
| `text_mancante` | manca del tutto il campo del testo |
| `text_troppo_lungo` | il testo supera i 3000 caratteri |

Alcuni controlli meritano una spiegazione.

L'autore e la chiave del record vengono controllati con particolare attenzione perché servono a costruire l'`uri` del post, cioè l'identificativo usato in tutta la pipeline per riconoscere i duplicati. Un `uri` costruito con dati sbagliati comprometterebbe tutto ciò che viene dopo.

L'orario viene controllato in due passaggi. Il primo verifica che la stringa abbia la forma di una data, il secondo che sia una data realmente esistente: una stringa come `2026-13-45T99:00:00Z` ha la forma giusta, ma non corrisponde a nessun giorno. Il controllo è importante perché l'orario decide in quale minuto viene conteggiato il post.

Sul testo c'è una distinzione sottile. Un testo vuoto è valido secondo le regole di Bluesky, e infatti viene solo scartato per scelta (sezione 3.3 della relazione). Un testo del tutto assente, invece, indica un evento malformato. Il limite di 3000 caratteri, infine, è dieci volte quello di Bluesky: lascia ampio margine a emoji e a post importati da altre piattaforme, ma ferma i testi chiaramente anomali.


Gli eventi che non superano i controlli finiscono nel topic `posts.dlq`, che funziona da **dead letter queue**: un posto dove mettere da parte i messaggi che non si riescono a elaborare, invece di perderli. Ogni messaggio contiene il motivo dello scarto, il momento in cui Logstash l'ha ricevuto e l'evento originale, oppure la riga così come è arrivata se non era nemmeno un JSON valido:

```json
{
  "motivo": "did_non_valido",
  "ricevuto_alle": "2026-10-21T15:08:24.112Z",
  "payload": { "did": "utente-senza-prefisso", "operation": "create", ... }
}
```

Nessuna fase della pipeline legge questo topic: serve a chi sviluppa o gestisce il sistema per capire cosa non va. Il volume atteso è molto basso, perché Jetstream invia dati generalmente corretti. Proprio per questo, un aumento improvviso dei messaggi nella dead letter queue è un segnale utile: può indicare che Bluesky ha cambiato il formato dei dati o che un'app sta inviando record non standard.


## A.4 Partizioni, compressione e limiti dell'ingestione

Per `posts.raw` il vantaggio delle tre partizioni oggi è soprattutto potenziale, perché Q1 elabora un blocco alla volta per sfruttare al meglio la GPU; la pipeline di Logstash che indicizza `posts.enriched` usa invece tre lettori, uno per partizione. Logstash comprime i messaggi dei post prima di scriverli su Kafka e li invia a piccoli gruppi, riducendo spazio e carico sul disco a fronte di un ritardo di poche decine di millisecondi.

La pipeline di ingestione ha alcuni limiti, accettati consapevolmente:

- **Controllo dei tipi.** Logstash, senza scrivere codice aggiuntivo, verifica che un campo esista e abbia la forma giusta, ma non il suo tipo: un testo inviato come oggetto invece che come stringa arriverebbe comunque a Kafka. Il controllo viene quindi ripetuto da Q1, che legge i messaggi con uno schema esplicito e scarta quelli con campi del tipo sbagliato.
- **Nessuna coda su disco in ingresso.** Le pipeline che scrivono su Elasticsearch hanno una coda su disco, quella di ingestione no. Se Logstash si ferma, gli eventi in transito vanno persi. Una coda su disco non risolverebbe il problema, perché il producer non conserva gli eventi già inviati e non potrebbe comunque rimandarli, mentre aggiungerebbe scritture su disco per ogni evento.
- **Ripresa dopo un'interruzione.** Il producer, quando si riconnette, riparte dagli eventi attuali e non da dove si era fermato. Un'interruzione produce quindi un buco nei dati. La pipeline lo sa gestire, perché il detector riconosce i buchi e non li scambia per un crollo del sentiment (capitolo 6), ma i post di quel periodo non vengono recuperati. Jetstream permetterebbe di riprendere da un punto preciso tramite un cursore, ed è uno dei possibili miglioramenti futuri.

## A.5 Il messaggio in posts.enriched

```json
{
  "uri": "at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/3myfgj6zbwh2o",
  "did": "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
  "time": "2026-10-21T15:08:23.966505Z",
  "text": "Just saw the news about the earthquake, hope everyone is safe",
  "link_title": "Strong earthquake hits...",
  "sentiment": -0.41,
  "sentiment_label": "negative",
  "model": "cardiffnlp/twitter-roberta-base-sentiment-latest+ov:fp32@GPU"
}
```

Il campo `langs` non viene copiato, perché è servito solo al filtro sulla lingua.

## A.6 Il messaggio in sentiment.1m

Valori illustrativi, coerenti con la sessione analizzata:

```json
{
  "window_start": "2026-11-08T19:15:00Z",
  "window_end": "2026-11-08T19:16:00Z",
  "n_posts": 1772,
  "n_authors": 1631,
  "mean_sentiment": 0.018,
  "std_sentiment": 0.52,
  "share_negative": 0.297,
  "share_positive": 0.281,
  "model": "cardiffnlp/twitter-roberta-base-sentiment-latest+ov:fp32@GPU"
}
```

`std_sentiment` è la deviazione standard campionaria e vale `null` se la finestra contiene un solo post, caso che con il traffico reale non capita mai.

## A.7 I campi aggiunti in sentiment.monitor

Il topic **`sentiment.monitor`** riceve una riga per ogni finestra elaborata. Contiene tutti i campi di `sentiment.1m` più lo stato del detector in quel minuto:

| Campo | Significato |
|---|---|
| `author_ratio` | rapporto autori/post, l'indicatore di spam |
| `baseline_mean`, `baseline_std` | valore tipico e variabilità del sentiment prima di questa finestra |
| `z_sentiment` | scostamento del sentiment in deviazioni standard |
| `baseline_volume`, `z_volume` | volume tipico in post al minuto e scostamento del volume |
| `warmup` | vero finché il detector non ha abbastanza finestre valide |
| `is_shock` | vero se la finestra è anomala |
| `volume_spike` | vero se il volume è esploso, indipendentemente dal sentiment |
| `skip_reason` | motivo per cui la finestra è stata scartata, oppure null |
| `alert_id` | identificativo dell'episodio in corso, oppure null |

`author_ratio` vale `n_authors / n_posts`; `baseline_volume` vale $e^{\mu_v} - 1$, cioè il volume tipico in post al minuto. Le finestre scartate o in warmup hanno comunque i loro z-score, utili nei grafici, ma non possono mai essere anomale.

## A.8 Il messaggio di chiusura su alerts

Il messaggio di chiusura dell'episodio delle 23:42 (sezione 6.7 della relazione), con valori arrotondati:

```json
{
  "alert_id": "shock-2026-11-03T23:42:00Z",
  "status": "closed",
  "start": "2026-11-03T23:42:00Z",
  "direction": "negative",
  "peak_z": -11.60,
  "peak_window": "2026-11-03T23:42:00Z",
  "peak_mean_sentiment": -0.075,
  "baseline_mean": 0.0496,
  "baseline_std": 0.0107,
  "peak_n_posts": 2100,
  "z_volume": 0.92,
  "n_shock_windows": 4,
  "model": "cardiffnlp/twitter-roberta-base-sentiment-latest+ov:fp32@GPU",
  "params": {
    "alpha": 0.03, "k": 3.5, "k_low": 2.5, "k_vol": 4.0, "clip_k": 3.5,
    "sigma_min_sentiment": 0.01, "sigma_min_volume": 0.05,
    "min_posts": 300, "min_authors": 250, "r_min": 0.8,
    "partial_ratio": 0.5, "warmup_n": 40,
    "close_after": 5, "gap_reset_minutes": 60.0
  },
  "end": "2026-11-03T23:46:00Z",
  "close_reason": "normal"
}
```

Il campo `z_volume` si riferisce alla finestra del picco. Il messaggio `open` contiene gli stessi campi tranne `end` e `close_reason`, con i valori noti all'apertura (`n_shock_windows` vale 1). `close_reason` può valere `normal`, `reversal` o `gap`.

Il significato dei parametri in `params`:

| Parametro | Valore | Significato |
|---|---|---|
| `alpha` | 0,03 | velocità con cui la baseline segue i dati recenti |
| `k` | 3,5 | soglia di `z_sentiment` per uno shock forte |
| `k_low` | 2,5 | soglia di `z_sentiment` per uno shock moderato... |
| `k_vol` | 4 | ...confermato da `z_volume` oltre questa soglia |
| `clip_k` | 3,5 | taglio della winsorizzazione, in deviazioni standard |
| `sigma_min_sentiment` | 0,01 | variabilità minima del sentiment |
| `sigma_min_volume` | 0,05 | variabilità minima del log-volume |
| `min_posts` | 300 | post minimi perché una finestra sia valida |
| `min_authors` | 250 | autori minimi perché una finestra sia valida |
| `r_min` | 0,8 | rapporto autori/post minimo |
| `partial_ratio` | 0,5 | sotto questa frazione del volume tipico la finestra è incompleta |
| `warmup_n` | 40 | finestre valide necessarie prima di segnalare shock |
| `close_after` | 5 | finestre normali consecutive che chiudono un episodio |
| `gap_reset_minutes` | 60 | buco oltre il quale la baseline riparte da zero |
