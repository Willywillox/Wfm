# Wfm

Repository con script di analisi e forecasting per il traffico call center.

- `analisi_trafficonewfct_profsari.py`: script principale di analisi e generazione forecast.
- `FORECAST_REVIEW.md`: sintesi dei punti di forza e delle aree di miglioramento del flusso di forecast.
- `requirements.txt`: dipendenze consigliate per eseguire lo script.
- `forecast_tutti_modelli.xlsx`: file generato a ogni run con il forecast combinato di **tutti** i modelli disponibili e la
  colonna `BEST_FORECAST` già impostata sul modello con MAPE più bassa dal backtest rolling.

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
- **Foglio `Metriche_Backtest`**: tabella MAE/MAPE/SMAPE per ciascun modello valutato.
- **Foglio `Sintesi`**: riepilogo del modello migliore e dell’elenco di modelli valutati.

Se il backtest non è stato eseguito (dati o dipendenze insufficienti), la colonna `BEST_FORECAST` non viene popolata e il file
resta comunque utile per confrontare visivamente tutte le curve.

### Perché vedi solo tre modelli nel file di backtest
`valutazione_forecast.xlsx` contiene **solo Holt-Winters, Naive e MA7** per mantenere il backtest rolling veloce e indipendente dalle librerie opzionali (Prophet, TBATS, ecc.). Gli altri modelli vengono comunque eseguiti nella fase di “FORECAST multi-modello”: verifica nel log “RIEPILOGO STATO MODELLI” che abbiano prodotto output (✅) e usa `forecast_confronto_modelli.xlsx` / `confronto_modelli_forecast.png` per confrontarli. Se hai tutte le dipendenze installate, il blocco finale “ESECUZIONE BACKTEST (rolling origin)” calcola le metriche anche per quei modelli riusciti.
