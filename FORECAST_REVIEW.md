# Analisi del modulo di forecasting

## Panoramica
Il file `analisi_trafficonewfct_profsari.py` implementa un set articolato di modelli per la previsione del traffico call center. Il flusso principale `genera_forecast_modelli` orchestri fino a sette approcci diversi (Holt-Winters, pattern fallback, naïve, SARIMA, Prophet, TBATS e intraday dinamico) e salva i risultati in Excel e grafici dedicati, costruendo anche un confronto unico tra i modelli disponibili.

## Punti di forza
- **Redundancy e resilienza**: ogni modello più complesso ha un fallback conservativo (media mobile/trend semplice) che evita crash in caso di dati insufficienti o dipendenze mancanti.
- **Holt-Winters multi-livello**: viene applicato sia a livello settimanale sia giornaliero con stima di R² e bande di confidenza, integrato con distribuzione intraday basata sui pattern storici.
- **Gestione delle festività per Prophet**: viene generato un calendario ricco di festività italiane, includendo pre/post-festivi e periodi estesi natalizi/capodanno, rendendo il modello sensibile a chiusure/riaperture particolari.
- **Modelli specializzati**: la presenza di TBATS (stagionalità multiple) e di un forecast intraday dinamico per fascia oraria permette di catturare pattern complessi e interazioni giorno×fascia quando i dati e le librerie lo consentono.

## Criticità osservate
- **Affidabilità su dataset piccoli**: diversi modelli vengono disattivati se mancano librerie o dati sufficienti, ma non esiste un controllo ex-ante della copertura finale (es. se tutti i modelli falliscono si ottengono DataFrame vuoti senza alert centralizzato).
- **Intervalli di confidenza empirici**: i fallback usano CI arbitrari (±15% o ±15%/±30% circa) senza calibrazione storica, riducendo l’affidabilità statistica delle bande prodotte.
- **Assenza di validazione incrociata**: non vengono calcolati errori storici (MAE/MAPE/SMAPE) per selezionare il modello migliore o per valutare la bontà del forecast ex-post, rendendo il confronto tra modelli solo visivo.
- **Gestione manuale delle dipendenze**: l’import di TBATS e Prophet è “best-effort” ma senza suggerire alternative quando entrambi mancano; inoltre la logica di debug è molto verbosa e potrebbe offuscare i log operativi.

## Raccomandazioni
1. **Aggiungere metriche di backtesting**: eseguire una validazione rolling (es. train-test split temporale) per stimare errori per ciascun modello e scegliere automaticamente il migliore per l’orizzonte richiesto.
2. **Uniformare gli intervalli di confidenza**: derivare le CI dai residui dei fallback (es. bootstrap o varianza empirica sui pattern per fascia) invece di usare percentuali fisse.
3. **Segnalare gli esiti di ogni modello**: produrre un riepilogo finale con stato (success/fail) e motivazione per ciascun metodo, evitando confronti vuoti e facilitando il troubleshooting.
4. **Ridurre la verbosità di debug**: spostare le stampe di diagnostica (import TBATS, ecc.) dietro a un flag `verbose` o un logger configurabile, mantenendo puliti i log di produzione.
