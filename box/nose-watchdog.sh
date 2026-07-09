#!/usr/bin/env bash
# Cane da guardia del Naso (snoopy-nose). Ogni 5 min via cron controlla l'eta'
# dell'ultimo dato 'aria' in InfluxDB: se supera THRESHOLD manda UN Telegram di
# allarme (una volta sola, via state file) e uno di recovery quando il Naso
# torna a comunicare. Riusa bot @MakSnoopyFetchBot e i token gia' sul box.
set -uo pipefail

BOT_TOKEN="$(grep -m1 '^TELEGRAM_BOT_TOKEN=' /home/mak/snoopy-fetch/.env | cut -d= -f2-)"
CHAT_ID="7953577281"
INFLUX_TOKEN="$(grep -m1 '^INFLUX_READ_TOKEN=' /home/mak/esp-naso/.env | cut -d= -f2-)"
ORG="snoopy-lab"
BUCKET="aria"
THRESHOLD=1200                        # 20 min (15 = reboot ESP32 senza WiFi, +5 di margine)
STATE_FILE="/home/mak/nose-watchdog.state"
SNOOZE_FILE="/home/mak/watchdog.snooze"   # epoch futuro -> guardiano in pausa (condiviso coi fratelli)

# Snooze condiviso: se c'e' un epoch futuro nel file, salta il giro (auto-riarmo allo scadere).
[ -f "$SNOOZE_FILE" ] && [ "$(cat "$SNOOZE_FILE" 2>/dev/null || echo 0)" -gt "$(date -u +%s)" ] && exit 0

flux() {
  docker exec -i influxdb influx query --org "$ORG" --token "$INFLUX_TOKEN" --raw "$1" 2>/dev/null
}
send() {
  curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${CHAT_ID}" --data-urlencode "text=$1" >/dev/null
}
fmt() { printf "%dh %02dm" $(( $1 / 3600 )) $(( ($1 % 3600) / 60 )); }

LAST_TIME=$(flux 'from(bucket: "'"$BUCKET"'")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "aria")
  |> last()
  |> keep(columns: ["_time"])' \
  | grep -E '^,[^,]*,[^,]*,[0-9]' | tail -1 | awk -F, '{gsub(/\r/,"",$4); print $4}')

NOW_EPOCH=$(date -u +%s)
if [ -z "$LAST_TIME" ]; then
  AGE=$THRESHOLD                       # nessun dato in 6h -> trattalo come giu'
  LAST_HM="oltre 6h fa"
else
  LAST_EPOCH=$(date -d "$LAST_TIME" +%s)
  AGE=$(( NOW_EPOCH - LAST_EPOCH ))
  LAST_HM=$(date -d "$LAST_TIME" '+%H:%M %d-%m')
fi

PREV="up"; PREV_EPOCH=0
if [ -f "$STATE_FILE" ]; then
  read -r PREV PREV_EPOCH < "$STATE_FILE" || true
  PREV="${PREV:-up}"; PREV_EPOCH="${PREV_EPOCH:-0}"
fi

if [ "$AGE" -ge "$THRESHOLD" ]; then
  if [ "$PREV" != "down" ]; then
    send "🚨 Naso muto da $(fmt "$AGE")! Ultimo dato: ${LAST_HM}. (soglia $((THRESHOLD/60)) min)"
    echo "down $NOW_EPOCH" > "$STATE_FILE"
  fi
else
  if [ "$PREV" = "down" ]; then
    OUT=$(( NOW_EPOCH - PREV_EPOCH ))
    send "✅ Naso di nuovo online. Era muto da ~$(fmt "$OUT"). Ultimo dato: ${LAST_HM}."
  fi
  echo "up $NOW_EPOCH" > "$STATE_FILE"
fi
