# Dashboard Grafana

Export JSON delle dashboard (senza `id`/`version`, pronte per l'import).
Per (ri)pubblicarne una via API:

```sh
ssh snoopybox 'curl -sS -u <user>:<pass> -H "Content-Type: application/json" \
  -X POST http://localhost:3000/api/dashboards/db \
  -d "{\"dashboard\": '"$(cat grafana/<file>.json)"', \"overwrite\": true}"'
```

| File | uid | Contenuto |
|---|---|---|
| `snoopy-nose-meteo.json` | `snoopy-nose-meteo` | Stazione meteo: estremi del periodo (stat + tabella con orari), grafico temp/umid/pressione, confronto "stessa ora, giorni scorsi" (-1/-2/-3/-7g via timeShift) |

Le altre dashboard (`snoopy-nose-v1`, `snoopy-nose-wifi`, `snoopy-nose-wifiscout`)
non sono ancora versionate qui; i backup vivono in `/home/mak/grafana-backup/` sul box.
