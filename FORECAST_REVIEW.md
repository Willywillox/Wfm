# Analisi del modulo di forecasting

## Panoramica
Il file `analisi_trafficonewfct_profsari.py` implementa un set articolato di modelli per la previsione del traffico call center. Il flusso principale `genera_forecast_modelli` orchestri fino a sette approcci diversi (Holt-Winters, pattern fallback, naïve, SARIMA, Prophet, TBATS e intraday dinamico) e salva i risultati in Excel e grafici dedicati, costruendo anche un confronto unico tra i modelli disponibili.【F:analisi_trafficonewfct_profsari.py†L1455-L1519】

## Punti di forza
- **Redundancy e resilienza**: ogni modello più complesso ha un fallback conservativo (media mobile/trend semplice) che evita crash in caso di dati insufficienti o dipendenze mancanti.【F:analisi_trafficonewfct_profsari.py†L845-L902】【F:analisi_trafficonewfct_profsari.py†L1059-L1099】
- **Holt-Winters multi-livello**: viene applicato sia a livello settimanale sia giornaliero con stima di R² e bande di confidenza, integrato con distribuzione intraday basata sui pattern storici.【F:analisi_trafficonewfct_profsari.py†L684-L842】【F:analisi_trafficonewfct_profsari.py†L905-L945】
- **Gestione delle festività per Prophet**: viene generato un calendario ricco di festività italiane, includendo pre/post-festivi e periodi estesi natalizi/capodanno, rendendo il modello sensibile a chiusure/riaperture particolari.【F:analisi_trafficonewfct_profsari.py†L1183-L1280】【F:analisi_trafficonewfct_profsari.py†L1283-L1358】
- **Modelli specializzati**: la presenza di TBATS (stagionalità multiple) e di un forecast intraday dinamico per fascia oraria permette di catturare pattern complessi e interazioni giorno×fascia quando i dati e le librerie lo consentono.【F:analisi_trafficonewfct_profsari.py†L947-L1041】【F:analisi_trafficonewfct_profsari.py†L1398-L1425】

## Criticità osservate
- **Affidabilità su dataset piccoli**: diversi modelli vengono disattivati se mancano librerie o dati sufficienti, ma non esiste un controllo ex-ante della copertura finale (es. se tutti i modelli falliscono si ottengono DataFrame vuoti senza alert centralizzato).【F:analisi_trafficonewfct_profsari.py†L952-L1000】【F:analisi_trafficonewfct_profsari.py†L1458-L1504】
- **Intervalli di confidenza empirici**: i fallback usano CI arbitrari (±15% o ±15%/±30% circa) senza calibrazione storica, riducendo l’affidabilità statistica delle bande prodotte.【F:analisi_trafficonewfct_profsari.py†L845-L902】【F:analisi_trafficonewfct_profsari.py†L1072-L1092】
- **Assenza di validazione incrociata**: non vengono calcolati errori storici (MAE/MAPE/SMAPE) per selezionare il modello migliore o per valutare la bontà del forecast ex-post, rendendo il confronto tra modelli solo visivo.【F:analisi_trafficonewfct_profsari.py†L1455-L1519】
- **Gestione manuale delle dipendenze**: l’import di TBATS e Prophet è “best-effort” ma senza suggerire alternative quando entrambi mancano; inoltre la logica di debug è molto verbosa e potrebbe offuscare i log operativi.【F:analisi_trafficonewfct_profsari.py†L53-L82】【F:analisi_trafficonewfct_profsari.py†L1486-L1500】

## Raccomandazioni
1. **Aggiungere metriche di backtesting**: eseguire una validazione rolling (es. train-test split temporale) per stimare errori per ciascun modello e scegliere automaticamente il migliore per l’orizzonte richiesto.【F:analisi_trafficonewfct_profsari.py†L1455-L1519】
2. **Uniformare gli intervalli di confidenza**: derivare le CI dai residui dei fallback (es. bootstrap o varianza empirica sui pattern per fascia) invece di usare percentuali fisse.【F:analisi_trafficonewfct_profsari.py†L845-L902】【F:analisi_trafficonewfct_profsari.py†L1072-L1092】
3. **Segnalare gli esiti di ogni modello**: produrre un riepilogo finale con stato (success/fail) e motivazione per ciascun metodo, evitando confronti vuoti e facilitando il troubleshooting.【F:analisi_trafficonewfct_profsari.py†L1458-L1519】【F:analisi_trafficonewfct_profsari.py†L952-L1000】
4. **Ridurre la verbosità di debug**: spostare le stampe di diagnostica (import TBATS, ecc.) dietro a un flag `verbose` o un logger configurabile, mantenendo puliti i log di produzione.【F:analisi_trafficonewfct_profsari.py†L53-L82】

