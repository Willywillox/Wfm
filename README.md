# Wfm

Repository con script di analisi e forecasting per il traffico call center.

- `analisi_trafficonewfct_profsari.py`: script principale di analisi e generazione forecast.
- `FORECAST_REVIEW.md`: sintesi dei punti di forza e delle aree di miglioramento del flusso di forecast.
- `requirements.txt`: dipendenze consigliate per eseguire lo script.
- `forecast_tutti_modelli.xlsx`: file generato a ogni run con il forecast combinato di **tutti** i modelli disponibili e la colonna `BEST_FORECAST`
  già impostata sul modello con MAPE più bassa dal backtest rolling multi-orizzonte. Include anche:
  - un foglio dedicato (`Best_<MODELLO>`) che riporta solo la curva del modello vincente e la sua metrica media (MAE/MAPE/SMAPE)
  - un ensemble automatico `ensemble_top2` (media dei due modelli con MAPE più bassa sull’orizzonte più vicino a quello richiesto) già presente nel file, così puoi confrontare anche una combinazione robusta dei migliori
  - `Metriche_per_Orizzonte` con MAE/MAPE/SMAPE per ciascun orizzonte (14/30/60/90 gg o l’orizzonte richiesto)
  - `monitoraggio_metriche.txt` (plain-text) che registra a ogni run il modello migliore e le sue metriche, utile per capire se le performance stanno migliorando o peggiorando nel tempo.

## Come valutare il modello migliore dal backtest
Lo script salva un file con le metriche di backtest (es. `valutazione_forecast.xlsx`) dove ogni riga rappresenta uno split rolling. Le colonne più utili sono:
- `HW_MAE/RMSE/MAPE`: errori del modello Holt-Winters (baseline stagionale/di trend).
- `Naive_*`: errore di un modello che riutilizza l’ultimo valore osservato.
- `MA7_*`: errore di una media mobile a 7 giorni.

Per scegliere rapidamente il forecast:
1. Ordina le righe per `HW_MAPE`, `Naive_MAPE` e `MA7_MAPE` separatamente e confronta i valori medi: la MAPE più bassa indica il modello più accurato in percentuale.
2. Se un modello ha MAPE significativamente più bassa e più stabile nelle varie righe, è il candidato principale. Nel campione riportato, Holt-Winters (`HW_*`) è sistematicamente più basso di `Naive_*` e `MA7_*`, quindi è il forecast consigliato.
3. Se le MAPE sono simili, preferisci il modello più semplice (Holt-Winters o media mobile) per robustezza; prova TBATS/Prophet solo se servono stagionalità complesse o festività e hanno metriche competitive.
4. Valuta anche MAE/RMSE per capire l’errore medio assoluto in unità di chiamate: valori più bassi significano previsioni più vicine ai dati reali.

Questa lettura, insieme al riepilogo di stato dei modelli stampato in console, ti indica quale forecast usare per l’orizzonte richiesto.

### File unico con tutti i forecast e il migliore già scelto
Oltre ai singoli file per metodo e al confronto `forecast_confronto_modelli.xlsx`, ogni run salva `forecast_tutti_modelli.xlsx`
con:
- **Foglio `Forecast_Tutti_Modelli`**: tutte le curve forecast di ogni modello riuscito, più la colonna `BEST_FORECAST` che
  replica il modello con MAPE più bassa nel backtest rolling (se le metriche sono state calcolate).
- **Foglio `Best_<MODELLO>`**: solo la curva del modello vincente (MAPE più bassa) per copiarla rapidamente in altri fogli.
- **Foglio `Metriche_Backtest`**: tabella MAE/MAPE/SMAPE per ciascun modello valutato e media complessiva.
- **Foglio `Metriche_per_Orizzonte`**: dettaglio MAE/MAPE/SMAPE per ciascun orizzonte testato (14/30/60/90 giorni e quello richiesto), ordinato per MAPE crescente.
- **Foglio `Sintesi`**: riepilogo del modello migliore, le sue metriche medie e l’elenco di modelli valutati.

Se il backtest non è stato eseguito (dati o dipendenze insufficienti), la colonna `BEST_FORECAST` non viene popolata e il file
resta comunque utile per confrontare visivamente tutte le curve.

### Perché vedi solo tre modelli nel file di backtest
`valutazione_forecast.xlsx` contiene **solo Holt-Winters, Naive e MA7** per mantenere il backtest rolling veloce e indipendente dalle librerie opzionali (Prophet, TBATS, ecc.). Gli altri modelli vengono comunque eseguiti nella fase di “FORECAST multi-modello”: verifica nel log “RIEPILOGO STATO MODELLI” che abbiano prodotto output (✅) e usa `forecast_confronto_modelli.xlsx` / `confronto_modelli_forecast.png` per confrontarli. Se hai tutte le dipendenze installate, il blocco finale “ESECUZIONE BACKTEST (rolling origin)” calcola le metriche anche per quei modelli riusciti.

### Novità per una previsione più affidabile
- **Intervalli di confidenza calibrati sui residui**: le bande LOW/HIGH sono ora basate sui quantili empirici dei residui (non più su una percentuale fissa), così riflettono la reale variabilità storica.
- **Backtest multi-orizzonte**: le metriche rolling vengono calcolate su 14/30/60/90 giorni (oltre all’orizzonte richiesto). Il modello “migliore” viene scelto sul MAPE dell’orizzonte più vicino a quello chiesto, ma trovi tutte le metriche nel foglio `Metriche_per_Orizzonte`.
- **Monitoraggio run-to-run**: `monitoraggio_metriche.txt` elenca timestamp, modello vincente e MAPE migliore per orizzonte, così puoi vedere se le performance stanno migliorando dopo ogni aggiornamento dati.
- **Ensemble automatico**: quando almeno due modelli hanno metriche disponibili, viene creata la curva `ensemble_top2` come media dei migliori due (per MAPE sull’orizzonte più vicino). È utile se nessun modello singolo domina nettamente o se vuoi un forecast più stabile.

### Checklist rapida: quando considerare “pronto per la produzione”
- **Dipendenze opzionali disponibili**: Prophet e TBATS installati (`pip install prophet tbats holidays`) per coprire stagionalità complesse e festività. Se mancano, il best model può ricadere su Holt-Winters o ensemble e perdere parte della stagionalità.
- **Backtest eseguito su tutti gli orizzonti**: controlla che `Metriche_per_Orizzonte` abbia valori per 14/30/60/90 giorni e per l’orizzonte richiesto; se mancano, il dataset potrebbe essere troppo corto o qualche modello non ha convergito.
- **Ensemble calcolato**: verifica nel riepilogo console che `ensemble_top2` sia stato creato e che compaia in `forecast_tutti_modelli.xlsx`; è il modo più sicuro per ottenere un forecast stabile se i singoli modelli sono vicini.
- **Intervalli credibili**: assicurati che le colonne LOW/HIGH non siano piatte o vuote; se succede, vuol dire che i residui erano pochi e le bande sono state approssimate con lo std della serie.
- **Monitoraggio coerente**: apri `monitoraggio_metriche.txt` e verifica che la MAPE del modello vincente non stia peggiorando run dopo run; in caso contrario aumenta la finestra storica o ricalibra i parametri (es. stagionalità) prima di usare il forecast.

Se tutte le voci sopra sono soddisfatte, il flusso è nelle migliori condizioni per restituire forecast affidabili; in caso contrario il sistema resta utilizzabile ma con margini di errore più ampi.
