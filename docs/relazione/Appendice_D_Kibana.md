# Appendice D – Le dashboard di Kibana

Questa appendice descrive in dettaglio le dashboard riassunte nel capitolo 10 della [relazione](Relazione.md).

## D.1 Concetti di base e avvio

Per usarlo bastano pochi concetti:

- **Data view.** Dice a Kibana quali indici guardare e quale campo contiene l'orario. Senza una data view non si vede nulla.
- **Discover.** Mostra i documenti uno per uno, come una tabella filtrabile. Serve a esplorare e a leggere i singoli post.
- **Lens.** È lo strumento con cui si costruiscono i grafici: linee nel tempo, barre, tabelle.
- **Dashboard.** Raccoglie più grafici in una sola pagina, tutti legati allo stesso intervallo di tempo.

In ogni pagina c'è un **selettore di tempo**, per esempio "ultime 24 ore", e una barra in cui scrivere filtri con un linguaggio semplice chiamato KQL. Per esempio `sentiment_label : "negative"` mostra solo i post negativi, e `is_shock : true` solo i minuti anomali. Gli orari sono salvati in UTC, ma Kibana li mostra nel fuso orario del browser.

**L'avvio.** Kibana parte insieme al resto della pipeline. Quando è pronto, il servizio `kibana-init` importa le data view, i grafici e le dashboard dal file `kibana/dashboards.ndjson`, che fa parte del repository, e imposta come data view predefinita quella della serie al minuto. L'importazione si ripete a ogni avvio e sovrascrive gli oggetti con lo stesso identificativo. Così le dashboard sono sempre quelle della versione corrente del progetto, anche dopo aver cancellato tutti i dati con `docker compose down -v`. Chi vuole modificarle deve salvarle come copia, altrimenti al riavvio successivo tornano alla versione del file.

**Perché dashboard pronte nel repository.** Kibana salva dashboard e data view dentro Elasticsearch. Se le avessi costruite solo a mano dall'interfaccia, ogni reset dei dati le avrebbe cancellate, e chi scarica il progetto avrebbe dovuto ricostruirle da zero seguendo delle istruzioni. Salvarle in un file versionato insieme al codice significa che chiunque avvii la pipeline trova subito le stesse viste, configurate nello stesso modo. È lo stesso principio seguito per i topic di Kafka e gli indici di Elasticsearch, creati da servizi di inizializzazione (capitolo 3): la configurazione fa parte del progetto e non dipende da operazioni manuali.

## D.2 La dashboard "Panoramica"

La dashboard principale, "Bluesky sentiment - Panoramica", è pensata per seguire la situazione. Si apre sulle ultime 24 ore e si aggiorna da sola ogni minuto. I suoi pannelli seguono un ordine preciso, lo stesso che si seguirebbe ragionando: prima si controlla che i dati siano affidabili, poi si guarda il sentiment, poi il ragionamento del detector, infine gli episodi.


**Salute della pipeline.** I primi due pannelli dicono se i dati arrivano regolari. "Post al minuto e volume tipico" mostra il numero di post per minuto insieme al volume tipico stimato dal detector: un buco o un crollo improvviso indicano un'interruzione dell'ingestione, non un evento reale. "Diversità degli autori" mostra il rapporto tra autori e post, con una linea tratteggiata sulla soglia di 0,8 sotto la quale il detector scarta una finestra come probabile spam.

**Il sentiment.** "Sentiment medio e baseline" mette sulla stessa linea temporale il sentiment del minuto e il valore tipico atteso dal detector, così si vede subito quando il primo si stacca dal secondo. "Quota di post negativi e positivi" aiuta a leggere la media, perché distingue "più post negativi" da "meno post positivi".

**Il detector.** "Z-score del sentiment" mostra lo scostamento di ogni minuto dalla baseline, con quattro linee di riferimento: le soglie dello shock forte (±3,5) e quelle dello shock moderato (±2,5). "Z-score del volume" ha una linea a 4, la soglia che conferma uno shock moderato. "Minuti anomali e minuti in warmup" conta, per ogni intervallo, i minuti giudicati anomali e quelli in cui il detector non era ancora pronto. "Finestre scartate dal detector, per motivo" mostra quante finestre sono state scartate dai controlli di qualità e perché (capitolo 6).

Il grafico degli z-score ha un dettaglio importante. Quando si guarda un intervallo lungo, per esempio 24 ore, Kibana non disegna un punto per ogni minuto ma raggruppa i minuti in intervalli più ampi. Se mostrasse la media di ogni intervallo, un singolo minuto anomalo verrebbe diluito tra i minuti normali vicini e scomparirebbe dal grafico. Il pannello mostra invece il valore massimo e il valore minimo di ogni intervallo: un minuto oltre la soglia resta visibile a qualsiasi scala.

**Gli episodi.** In fondo alla dashboard ci sono due tabelle. La prima, "Alert (episodi di shock)", elenca gli episodi dal più recente, con stato, direzione, fine, picco dello z-score, numero di minuti anomali, volume al picco e motivo di chiusura. Poiché il messaggio di chiusura aggiorna il documento creato all'apertura (capitolo 6), la tabella mostra sempre lo stato attuale di ogni episodio. La seconda, "Spiegazioni degli shock (LLM)", mostra solo gli episodi spiegati, con la spiegazione dell'explainer accanto ai termini significativi e ai titoli dei link più condivisi (capitolo 7).

## D.3 La dashboard "Indagine sui post"

La seconda dashboard, "Bluesky sentiment - Indagine sui post", serve a passare dal "quando" al "perché" guardando direttamente i post. Si apre sull'ultima ora, ma l'uso tipico è restringere il selettore di tempo all'intervallo di un episodio.

Contiene quattro pannelli:

- **Composizione dei post per etichetta.** Barre che mostrano, per ogni intervallo, la percentuale di post negativi, neutri e positivi.
- **Link più condivisi.** I 15 titoli di link che compaiono più spesso nell'intervallo scelto.
- **Post più negativi.** I post con etichetta negativa, ordinati dal punteggio più basso, con testo e titolo del link.
- **Post più positivi.** Lo stesso, per i post positivi.

Come detto nel capitolo 7, la spiegazione dell'explainer è un'ipotesi. Con questa dashboard chi la legge può controllare in pochi secondi se i post e i link di quel momento la confermano.

## D.4 La dashboard "Sviluppo"

La terza dashboard, "Bluesky sentiment - Sviluppo", è uno strumento per chi lavora sulla pipeline. Mostra le ultime due ore, si aggiorna ogni dieci secondi e, a differenza della "Panoramica", mostra dati utili fin dai primi minuti dopo l'avvio, quando il detector è ancora in warmup.

I suoi pannelli controllano le fasi una per una:

- **Post indicizzati al minuto.** Conta i post arrivati in Elasticsearch, e si riempie pochi secondi dopo l'avvio di Q1.
- **Post per finestra secondo Q2.** Mostra lo stesso conteggio calcolato da Q2. Deve somigliare al pannello precedente, ma con due o tre minuti di ritardo, dovuti al watermark (capitolo 5).
- **Sentiment medio dai post.** È calcolato direttamente sui singoli post, quindi controlla Q1 anche prima che Q2 pubblichi le prime finestre.
- **Post per motore.** Divide i post in base al campo `model`, e deve mostrare un solo valore, quello del motore in uso (capitolo 8). Due valori indicano che sono stati mescolati post calcolati con motori diversi.
- **Avanzamento del warmup** e **Finestre ricevute da Q3: valide e scartate.** Mostrano a che punto è il detector e se manca qualche minuto. Un minuto senza barra è un buco nell'ingestione.
- **Z-score anche durante il warmup** e **Minuti oltre soglia e shock segnalati.** Mostrano cosa vedrebbe il detector se non ci fossero warmup e controlli di qualità, e lo confrontano con gli shock davvero segnalati.
- **Due tabelle.** La prima contiene le ultime righe di `sentiment-1m` con tutti i campi del detector, la seconda gli ultimi post indicizzati.

---
