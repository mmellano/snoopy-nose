#!/usr/bin/env bash
# Cane da guardia WiFi del Naso (snoopy-nose). Ogni 5 min: se il Naso e' online
# ma in stato WiFi degradato manda UN Telegram (anti-spam via state file) e uno
# di recovery al rientro. Due guasti distinti:
#   (1) ripiegato sul ROUTER principale (non sente il repeater)
#   (2) RSSI medio (10m) <= RSSI_BAD
# Resta zitto se i dati non sono freschi: offline = competenza di nose-watchdog.sh
# (guardiano delle comunicazioni). Isteresi su RSSI per non flappare.
set -uo pipefail

BOT_TOKEN="$(grep -m1 '^TELEGRAM_BOT_TOKEN=' /home/mak/snoopy-fetch/.env | cut -d= -f2-)"
CHAT_ID="7953577281"
INFLUX_TOKEN="$(grep -m1 '^INFLUX_READ_TOKEN=' /home/mak/esp-naso/.env | cut -d= -f2-)"
ORG="snoopy-lab"; BUCKET="aria"

ROUTER_BSSID="1C:73:E2:46:13:F0"
REPEATER_BSSID="00:7A:A4:5E:65:45"
RSSI_BAD=-80          # <= -80 dBm = segnale 'da allarme'
RSSI_OK=-75           # rientro (isteresi: deve risalire sopra -75)
FRESH=600             # ultimo dato wifi piu' vecchio di 10 min -> offline, lascia perdere
STATE_FILE="/home/mak/nose-wifi-watchdog.state"
SNOOZE_FILE="/home/mak/watchdog.snooze"   # epoch futuro -> guardiano in pausa (condiviso coi fratelli)

# Snooze condiviso: se c'e' un epoch futuro nel file, salta il giro (auto-riarmo allo scadere).
[ -f "$SNOOZE_FILE" ] && [ "$(cat "$SNOOZE_FILE" 2>/dev/null || echo 0)" -gt "$(date -u +%s)" ] && exit 0

flux() { docker exec -i influxdb influx query --org "$ORG" --token "$INFLUX_TOKEN" --raw "$1" 2>/dev/null; }
send() { curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
           --data-urlencode "chat_id=${CHAT_ID}" --data-urlencode "text=$1" >/dev/null; }
fmt()  { printf "%dh %02dm" $(( $1 / 3600 )) $(( ($1 % 3600) / 60 )); }
# I data-row del CSV annotato InfluxDB hanno il campo result vuoto: ",,<table>,<val>".
# Match permissivo (vale anche se result=_result), esclude header/annotazioni.
val()  { echo "$1" | grep -E '^,[^,]*,[0-9]' | tail -1 | awk -F, -v c="$2" '{gsub(/\r/,"",$c); print $c}'; }

# --- freschezza: timestamp ultimo RSSI ---
LAST_T=$(val "$(flux 'from(bucket:"'"$BUCKET"'") |> range(start:-30m) |> filter(fn:(r)=>r._measurement=="wifi" and r._field=="value") |> last() |> keep(columns:["_time"])')" 4)
[ -z "$LAST_T" ] && exit 0
NOW=$(date -u +%s); LASTE=$(date -d "$LAST_T" +%s); AGE=$(( NOW - LASTE ))
[ "$AGE" -gt "$FRESH" ] && exit 0          # offline -> ci pensa il guardiano comunicazioni

# --- RSSI medio 10m + BSSID corrente ---
RAW=$(val "$(flux 'from(bucket:"'"$BUCKET"'") |> range(start:-10m) |> filter(fn:(r)=>r._measurement=="wifi" and r._field=="value") |> mean() |> keep(columns:["_value"])')" 4)
[ -z "$RAW" ] && exit 0
RSSI=$(printf "%.0f" "$RAW")
BSSID=$(val "$(flux 'from(bucket:"'"$BUCKET"'") |> range(start:-30m) |> filter(fn:(r)=>r._measurement=="wifi_net" and r.field=="wifi_bssid") |> last() |> keep(columns:["_value"])')" 4)

on_router=false; [ "$BSSID" = "$ROUTER_BSSID" ] && on_router=true
weak=false;      [ "$RSSI" -le "$RSSI_BAD" ] && weak=true
case "$BSSID" in
  "$REPEATER_BSSID") AP="repeater" ;;
  "$ROUTER_BSSID")   AP="router principale" ;;
  "")                AP="AP sconosciuto" ;;
  *)                 AP="AP $BSSID" ;;
esac

PREV="good"; PREV_EPOCH=0
if [ -f "$STATE_FILE" ]; then read -r PREV PREV_EPOCH < "$STATE_FILE" || true; PREV="${PREV:-good}"; PREV_EPOCH="${PREV_EPOCH:-0}"; fi

if [ "$PREV" != "bad" ]; then
  # attualmente sano -> entra in BAD se sul router OPPURE segnale debole
  if $on_router || $weak; then
    if $on_router && $weak; then
      MSG="📶🚨 Naso ripiegato sul ROUTER principale (non sente il repeater) E segnale che fa cagare: ~${RSSI} dBm!"
    elif $on_router; then
      MSG="📶🚨 Naso ripiegato sul ROUTER principale: non sta sentendo il repeater. RSSI ~${RSSI} dBm."
    else
      MSG="📶⚠️ Naso sul ${AP} ma segnale debole: ~${RSSI} dBm (soglia ${RSSI_BAD})."
    fi
    send "$MSG"
    echo "bad $NOW" > "$STATE_FILE"
  else
    echo "good $NOW" > "$STATE_FILE"
  fi
else
  # attualmente in allarme -> rientra solo se NON sul router E RSSI >= RSSI_OK (isteresi)
  if ! $on_router && [ "$RSSI" -ge "$RSSI_OK" ]; then
    OUT=$(( NOW - PREV_EPOCH ))
    send "📶✅ Naso WiFi di nuovo a posto: sul ${AP} a ~${RSSI} dBm. (degrado durato ~$(fmt "$OUT"))"
    echo "good $NOW" > "$STATE_FILE"
  fi
  # altrimenti resta in allarme, in silenzio
fi
