# Script del box (fuori Docker)

Questi script girano su snoopy-box direttamente da cron dell'utente `mak`,
deployati in `/home/mak/` (NON dentro il clone git del box). La copia nel repo
è la fonte di verità del codice; il deploy è manuale via scp.

| Script | Cron | Cosa fa |
|---|---|---|
| `naso-index.py` | `4-59/5 * * * *` | Indice aria compensata: fit OLS 7gg (temp+umid+deriva+armoniche), de-bias mediana 48h, scrive misura `gas_index` (v2 dal 2026-07-09) |
| `naso-detector.py` | `* * * * *` | Rilevatore eventi-puzza su `gas_index.z` (isteresi -1.8/-1.1), diario CSV, domande via @MakSnoopyNoseBot, annotazioni Grafana, mirror misura `evento`. Modalità `--replay <start> <stop>` per backtest senza effetti collaterali |
| `nose-watchdog.sh` | `*/5 * * * *` | Allarme Telegram se il Naso non pubblica da ≥20 min |
| `nose-wifi-watchdog.sh` | `2-59/5 * * * *` | Allarme se il Naso ripiega sul router o RSSI medio ≤ -80 dBm |

Tutti i cron girano con `flock` sul rispettivo `.lock` e loggano in `/home/mak/*.log`.
I due watchdog rispettano lo snooze condiviso `/home/mak/watchdog.snooze` (epoch futuro = salta il giro).

## Segreti

Niente segreti nel codice: gli script leggono da `/home/mak/esp-naso/.env`
(`INFLUX_TOKEN`, `INFLUX_READ_TOKEN`, `GRAFANA_CRED`) e da
`/home/mak/naso-bot.env` (`NASO_BOT_TOKEN`), più il token Telegram del bot
condiviso da `/home/mak/snoopy-fetch/.env`. Vedi `.env.example` in radice.

## Deploy

```sh
scp box/<script> snoopybox:/home/mak/<script>.new
ssh snoopybox 'sed -i "s/\r$//" /home/mak/<script>.new \
  && python3 -m py_compile /home/mak/<script>.new \
  && flock /home/mak/<lock-del-cron> -c "mv /home/mak/<script>.new /home/mak/<script>"'
```

Il `sed` toglie eventuali CRLF (i .sh con CRLF si inchiodano sul `#!`); il
`flock` evita di sostituire il file mentre il cron lo sta eseguendo. Prima di
sostituire, tenere un backup datato (`cp script script.bak-YYYYMMDD`).
