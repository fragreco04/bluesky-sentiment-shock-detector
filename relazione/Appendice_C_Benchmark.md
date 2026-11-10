# Appendice C – Benchmark completi di Q1

Questa appendice raccoglie le tabelle di tutte le prove descritte nel capitolo 8 della [relazione](Relazione.md). Tutte le misure sono state fatte fuori da Spark, su 2000 post reali presi da `posts.raw`, con lo script di benchmark.

## C.1 Il computer

| Componente | Dettagli |
|---|---|
| Processore | Intel Core i7-1165G7, 4 core fisici, 8 thread, 2,8 GHz |
| Memoria | 16 GB |
| Grafica | Intel Iris Xe, integrata nel processore |
| Ambiente | Docker dentro WSL2 su Windows |

## C.2 Note sul metodo

La fedeltà è misurata rispetto al modello originale eseguito con PyTorch fp32, con tre indicatori:

- **Etichette uguali.** La percentuale di post che ricevono la stessa etichetta (negativo, neutro o positivo) del modello originale.
- **Spostamento della media.** Di quanto cambia il sentiment medio dei 2000 post. È l'indicatore più importante per il progetto, perché il detector lavora proprio sulle medie.
- **Spostamento per classe.** Lo stesso spostamento, misurato separatamente sui post negativi, neutri e positivi. Questo indicatore l'ho aggiunto a metà del lavoro, e la sezione 8.4 della relazione spiega perché.

Le soglie per accettare una configurazione erano almeno il 97% di etichette uguali e uno spostamento della media sotto 0,01. Tra un'esecuzione e l'altra lo stesso modello può variare di qualche post al secondo, per la temperatura del portatile o per gli altri programmi aperti: per questo PyTorch compare nelle tabelle con valori leggermente diversi, tra 11,4 e 13,4 post al secondo, misurati in giorni diversi. I post hanno in media 28 token (mediana 20), e il limite di 256 token ne taglia solo lo 0,1%.

## C.3 PyTorch fp32

"2×2" significa due processi, ognuno con la sua copia del modello e due thread.

| Configurazione | Velocità |
|---|---|
| 1 processo × 4 thread | 11,4 post/s |
| 2 processi × 2 thread | 11,8 post/s |
| 4 processi × 1 thread | non completata, memoria insufficiente |

Il limite sono i quattro core fisici: più processi non aumentano la velocità e occupano solo più memoria.

## C.4 La quantizzazione int8 di PyTorch

Le tre varianti calcolano le scale dei pesi separatamente per ogni canale, lasciano in precisione piena lo strato finale che decide l'etichetta, oppure lasciano in precisione piena anche gli strati più delicati.

| Variante | Velocità | Etichette uguali | Spostamento della media |
|---|---|---|---|
| fp32 (riferimento) | 13,4 post/s | 100% | 0 |
| int8 | 31,2 post/s | 76,7% | +0,111 |
| int8, scale per canale | 30,4 post/s | 81,5% | +0,093 |
| int8, senza lo strato finale | 29,7 post/s | 81,4% | +0,093 |
| int8, senza gli strati più delicati | 19,9 post/s | 90,2% | +0,018 |

## C.5 OpenVINO sulla CPU

Le versioni quantizzate erano calibrate su 300 post diversi da quelli del benchmark.

| Motore | Velocità | Etichette uguali | Spostamento della media |
|---|---|---|---|
| PyTorch fp32 (riferimento) | 13,3 post/s | 100% | 0 |
| OpenVINO fp32 | 13,5 post/s | 100% | 0,000 |
| OpenVINO int8 | 43,4 post/s | 97,2% | +0,012 |
| OpenVINO int8 senza SmoothQuant | 44,4 post/s | 95,7% | +0,002 |

Spostamento per classe con diversi livelli di SmoothQuant:

| Variante | Etichette uguali | Spost. medio | Spost. negativi | Spost. neutri | Spost. positivi |
|---|---|---|---|---|---|
| int8, SmoothQuant 0,95 (default) | 97,2% | +0,012 | +0,024 | +0,016 | -0,005 |
| **int8, SmoothQuant 0,75** | **98,1%** | **+0,002** | **+0,004** | **+0,002** | **-0,002** |
| int8, SmoothQuant 0,50 | 94,6% | +0,021 | +0,030 | +0,028 | +0,001 |
| int8 senza SmoothQuant | 95,7% | +0,002 | +0,004 | +0,007 | -0,008 |

In un benchmark sulla CPU un processo con quattro thread sembrava più veloce di due processi con due thread (48 contro 41–44 post al secondo). Confrontandoli nella stessa esecuzione i valori sono risultati uguali (45,7 contro 45,4): la differenza precedente era solo una variazione casuale.

## C.6 La GPU integrata

| Motore | Velocità | Memoria | Etichette uguali | Spost. medio | Spost. negativi | Spost. positivi |
|---|---|---|---|---|---|---|
| PyTorch fp32 (riferimento) | 12,9 post/s | 1,6 GB | 100% | 0 | 0 | 0 |
| OpenVINO int8 (SQ 0,75) su CPU | 43–46 post/s | 1,4 GB | 98,1% | +0,002 | +0,004 | -0,002 |
| **OpenVINO fp32 su GPU** | **74,6 post/s** | **1,1 GB** | **99,8%** | **0,000** | **0,000** | **0,000** |
| OpenVINO int8 (SQ 0,75) su GPU | 66,9 post/s | 1,1 GB | 97,8% | 0,000 | -0,027 | +0,035 |

Per non perdere fedeltà, i pesi del modello sono salvati su disco nella loro forma completa a 32 bit, e non arrotondati come farebbe OpenVINO per default. L'int8 sulla GPU è più lento del fp32 perché questa scheda non trae vantaggio dai numeri a 8 bit.

## C.7 Riepilogo

| Tentativo | Velocità | Fedeltà | Esito |
|---|---|---|---|
| PyTorch fp32 (partenza) | circa 13 post/s | riferimento | troppo lento |
| PyTorch int8 | circa 20–31 post/s | 77–90% di etichette uguali | troppo impreciso |
| OpenVINO fp32 su CPU | circa 13 post/s | identica | nessun guadagno |
| OpenVINO int8 su CPU | circa 44 post/s | 98,1%, spostamento uniforme | buono, ma sotto l'obiettivo |
| OpenVINO int8 su GPU | circa 67 post/s | 97,8%, amplifica gli estremi | scartato |
| **OpenVINO fp32 su GPU** | **circa 75 post/s** | **99,8%, spostamento nullo** | **scelto** |

## C.8 Limiti

- **Nessuna alternativa veloce senza GPU.** Togliendo le varianti int8, l'unica alternativa alla GPU è il modello fp32 sulla CPU, a circa 13 post al secondo: troppo lento per il traffico reale. Su un computer senza GPU Intel la pipeline accumulerebbe ritardo. Se servisse, la variante int8 con SmoothQuant a 0,75, misurata a circa 44 post al secondo, si potrebbe reintrodurre.
- **Configurazione legata a questo computer.** Il compose collega a Q1 il dispositivo con cui WSL2 comunica con la GPU di Windows. Su un computer senza WSL2 o senza GPU Intel, Q1 non parte senza modificare il compose. La pipeline è quindi portabile per tutti i componenti tranne questo.
- **Benchmark limitato.** Le misure sono state fatte su 2000 post e su un solo computer, con variazioni di qualche post al secondo tra un'esecuzione e l'altra. Le differenze piccole tra configurazioni non vanno prese come definitive.
- **Recupero dell'arretrato non misurato.** Nella sessione reale Q1 non è mai rimasto indietro, quindi la velocità durante il recupero di un arretrato non è stata misurata dentro Spark.
- **Effetti sugli altri servizi.** CPU e GPU integrata condividono lo stesso limite di consumo del processore. Non ho ancora misurato se, con la GPU al lavoro per molte ore, la CPU rallenti abbastanza da pesare sugli altri servizi.
- **Picchi di traffico.** Il margine di due volte e mezzo è stato misurato su una serata con traffico stabile. Non so ancora quanto possa crescere il traffico durante un evento molto grande, e se il margine basti.
