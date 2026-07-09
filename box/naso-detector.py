#!/usr/bin/env python3
"""
Snoopy-Naso - rilevatore automatico di eventi-puzza (MODALITA OSSERVAZIONE).

Legge il residuo COMPENSATO dell'aria (gas_index.z, lo calcola naso-index.py),
rileva i cali sotto l'atteso, registra gli eventi nel diario CSV e -- via il bot dedicato
@MakSnoopyNoseBot -- chiede "che puzza e'?". La risposta e' OPZIONALE e puo'
arrivare in qualsiasi momento: usando il tasto "Rispondi" di Telegram sul
messaggio dell'evento, l'etichetta si aggancia all'evento giusto anche ore dopo
e fuori ordine.

Avviato 2026-06-02 in osservazione. Le soglie sono valori di partenza: vanno
tarate a burn-in finito (~03-04/06) sui dati reali / sui falsi positivi.
"""
import base64, csv, io, json, os, re, sys, time, urllib.request, urllib.parse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# --- percorsi ---
NASO_ENV   = "/home/mak/naso-bot.env"
INFLUX_ENV = "/home/mak/esp-naso/.env"
STATE_FILE = "/home/mak/naso-detector.state.json"
DIARY_CSV  = "/home/mak/diario-puzze.csv"

# --- InfluxDB ---
INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "snoopy-lab"
BUCKET     = "aria"

# --- Telegram ---
CHAT_ID = 7953577281            # Marco (stesso id su tutti i bot in chat privata)

# --- parametri rilevamento (COMPENSATO su gas_index.z) ---
# Il detector NON lavora piu' sul gas grezzo: di notte il gas SALE forte
# (raffreddamento) e MASCHERA i cali da puzza -> col -18% grezzo sfuggivano eventi
# reali (sera 2026-06-05: 3 cali annusati/probabili, zero aperti). Ora si legge il
# RESIDUO COMPENSATO `gas_index.z` (naso-index.py, ogni 5 min): gas_index gia'
# scorpora temperatura/umidita'/ciclo giorno-notte, quindi una puzza = z che scende
# sotto l'atteso, indipendente dal trend. z e' in sigma (negativo = peggio
# dell'atteso). Resta fedele all'idea originale ("ignora le derive lente"): qui sono
# gia' rimosse per costruzione.
Z_OPEN           = -1.8       # apre se z <= -1.8 sigma sotto l'atteso
Z_CLOSE          = -1.1       # chiude quando z risale sopra -1.1 (isteresi)
Z_ONSET          = -1.2       # spalla per la retro-datazione: z ~ tornato normale
CONFIRM_N        = 1          # campioni (gas_index e' gia' a 5 min, mediato -> 1 basta)
COOLDOWN_S       = 300        # niente riapertura entro 5 min dalla chiusura (anti-flap)
MAX_ASKS         = 100        # quante domande "in sospeso" tenere mappate
ONSET_LOOKBACK_S = 25 * 60    # non retro-datare l'inizio oltre 25 min
INDEX_STALE_S    = 20 * 60    # se l'ultimo z e' piu' vecchio di cosi', non agire
# Soglie tarate 2026-06-06 sui 3 eventi etichettati da Marco la sera del 05/06:
#   verde (annusato, sicuro)   z -1.97 -> CATTURATO
#   arancio (probabile, 00:25) z -2.13 -> CATTURATO
#   arancio (probabile, 23:24) z -1.64 -> sotto soglia (resta "probabile")
# Piu' sensibile: alza Z_OPEN verso -1.5; piu' prudente: -2.0. Verifica col --replay.
TZ = ZoneInfo("Europe/Rome")

# --- Grafana (auto-annotazioni sul pannello gas) ---
GRAFANA_URL  = "http://localhost:3000"
# credenziali "user:password" in GRAFANA_CRED dentro INFLUX_ENV (repo pubblico:
# niente segreti nel codice); se manca, le annotazioni vengono saltate con log
DASH_UID     = "snoopy-nose-v1"
PANEL_ID     = 9                  # "Resistenza Gas nel tempo"

CSV_HEADER = ["event_id", "data", "ora_inizio", "ora_fine", "durata_min",
              "profondita_pct", "intensita", "natura", "note"]


# ----------------------------------------------------------------------------- util
def parse_env(path, key):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith(key + "="):
                    v = line[len(key) + 1:].strip()
                    if len(v) >= 2 and v[0] in "\"'" and v[-1] == v[0]:
                        v = v[1:-1]
                    return v
    except FileNotFoundError:
        pass
    return None


def rfc3339_epoch(s):
    s = s.strip()
    m = re.match(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:?\d\d)?$', s)
    if not m:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()
    base, frac, tz = m.group(1), (m.group(2) or ''), (m.group(3) or 'Z')
    if frac:
        frac = frac[:7]            # punto + max 6 cifre (fromisoformat non regge i nanosecondi)
    if tz == 'Z':
        tz = '+00:00'
    return datetime.fromisoformat(base + frac + tz).timestamp()


def hhmm(epoch):
    return datetime.fromtimestamp(epoch, TZ).strftime("%H:%M")


# ------------------------------------------------------------------------- InfluxDB
def influx_index_series(token, range_expr="start:-6h"):
    """Serie del residuo compensato dal gas_index: lista di (epoch, z, resid_pct),
    ordinata. z in sigma (negativo = aria peggio dell'atteso); resid_pct = scarto %
    dal gas atteso. `range_expr` e' il corpo di range() (es. 'start:-6h' live, oppure
    'start:..., stop:...' per il --replay)."""
    flux = (f'from(bucket:"{BUCKET}") '
            f'|> range({range_expr}) '
            f'|> filter(fn:(r)=> r._measurement=="gas_index" and '
            f'(r._field=="z" or r._field=="resid_pct")) '
            f'|> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value") '
            f'|> keep(columns:["_time","z","resid_pct"])')
    url = f"{INFLUX_URL}/api/v2/query?org={urllib.parse.quote(INFLUX_ORG)}"
    req = urllib.request.Request(url, data=flux.encode(),
                                 headers={"Authorization": f"Token {token}",
                                          "Content-Type": "application/vnd.flux",
                                          "Accept": "application/csv"})
    with urllib.request.urlopen(req, timeout=20) as r:
        text = r.read().decode()
    rows, idx = [], None
    for row in csv.reader(io.StringIO(text)):
        if not row or row[0].startswith('#'):
            continue
        if idx is None:                       # prima riga non-# = intestazione colonne
            if '_time' in row and 'z' in row and 'resid_pct' in row:
                idx = {c: i for i, c in enumerate(row)}
            continue
        ti, zi, ri = idx['_time'], idx['z'], idx['resid_pct']
        if len(row) <= max(ti, zi, ri) or not row[ti] or not row[zi] or not row[ri]:
            continue
        try:
            rows.append((rfc3339_epoch(row[ti]), float(row[zi]), float(row[ri])))
        except ValueError:
            continue
    rows.sort()
    return rows


def lp_str(s):
    """Escape per un field-string in line protocol InfluxDB."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def mirror_csv_to_influx(wtoken):
    """Specchia gli eventi CHIUSI del diario CSV nella misura `evento` (bucket aria),
    cosi' Grafana li mostra in tabella. Il CSV resta la fonte di verita': qui si
    riscrive tutto a ogni giro (upsert su stesso eid+timestamp -> sovrascrive), cosi'
    natura/correzioni via Telegram si propagano senza casi speciali."""
    if not os.path.exists(DIARY_CSV):
        return
    with open(DIARY_CSV, newline="") as f:
        rows = list(csv.reader(f))
    if len(rows) < 2:
        return
    idx = {c: i for i, c in enumerate(rows[0])}
    lines = []
    for r in rows[1:]:
        if not r or len(r) < len(rows[0]):
            continue
        dur, prof = r[idx["durata_min"]].strip(), r[idx["profondita_pct"]].strip()
        if not dur or not prof:                 # evento ancora aperto -> salta
            continue
        eid = r[idx["event_id"]].strip()
        try:
            ts = int(datetime.strptime(f'{r[idx["data"]]} {r[idx["ora_inizio"]]}',
                                       "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ).timestamp())
            float(dur); float(prof)
        except ValueError:
            continue
        inten = lp_str(r[idx["intensita"]].strip() or "?")
        natura = lp_str(r[idx["natura"]].strip())
        lines.append(f'evento,eid={eid} dur={float(dur)},prof={float(prof)},'
                     f'inten="{inten}",natura="{natura}" {ts}000000000')
    if not lines:
        return
    url = (f"{INFLUX_URL}/api/v2/write?org={urllib.parse.quote(INFLUX_ORG)}"
           f"&bucket={BUCKET}&precision=ns")
    req = urllib.request.Request(url, data="\n".join(lines).encode(),
                                 headers={"Authorization": f"Token {wtoken}",
                                          "Content-Type": "text/plain; charset=utf-8"})
    try:
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        print(f"[naso] mirror influx errore: {e}", file=sys.stderr)


# -------------------------------------------------------------------------- Telegram
def tg(method, params, token):
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[naso] tg {method} errore: {e}", file=sys.stderr)
        return None


def send(token, text, reply_to=None):
    p = {"chat_id": CHAT_ID, "text": text}
    if reply_to:
        p["reply_to_message_id"] = reply_to
    return tg("sendMessage", p, token)


def get_updates(token, offset):
    p = {"timeout": 0}
    if offset:
        p["offset"] = offset
    r = tg("getUpdates", p, token)
    return r["result"] if (r and r.get("ok")) else []


# -------------------------------------------------------------------------- Grafana
def grafana_req(method, path, payload=None):
    cred = parse_env(INFLUX_ENV, "GRAFANA_CRED")
    if not cred:
        raise RuntimeError(f"GRAFANA_CRED mancante in {INFLUX_ENV}")
    auth = base64.b64encode(cred.encode()).decode()
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(GRAFANA_URL + path, data=data, method=method,
                                 headers={"Authorization": "Basic " + auth,
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def grafana_annotate(start_epoch, end_epoch, text, tags):
    """Crea un'annotazione a regione (inizio->fine) sul pannello gas. -> id o None."""
    try:
        r = grafana_req("POST", "/api/annotations",
                        {"dashboardUID": DASH_UID, "panelId": PANEL_ID,
                         "time": int(start_epoch * 1000), "timeEnd": int(end_epoch * 1000),
                         "tags": tags, "text": text})
        return r.get("id")
    except Exception as e:
        print(f"[naso] grafana annotate errore: {e}", file=sys.stderr)
        return None


def grafana_update(annid, text, tags):
    """Aggiorna testo/tag di un'annotazione (es. quando arriva la natura via TG)."""
    try:
        grafana_req("PATCH", f"/api/annotations/{annid}", {"text": text, "tags": tags})
    except Exception as e:
        print(f"[naso] grafana update errore: {e}", file=sys.stderr)


# ------------------------------------------------------------------------------ CSV
def ensure_csv():
    if not os.path.exists(DIARY_CSV):
        with open(DIARY_CSV, "w", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)


def csv_append_open(eid, start_epoch):
    dt = datetime.fromtimestamp(start_epoch, TZ)
    with open(DIARY_CSV, "a", newline="") as f:
        csv.writer(f).writerow([eid, dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"),
                                "", "", "", "", "", ""])


def csv_update(eid, **fields):
    """Aggiorna la riga dell'evento eid con i campi passati (per nome colonna)."""
    if not os.path.exists(DIARY_CSV):
        return
    with open(DIARY_CSV, newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return
    idx = {c: i for i, c in enumerate(rows[0])}
    for r in rows[1:]:
        if r and r[0] == str(eid):
            for k, v in fields.items():
                if k in idx:
                    r[idx[k]] = v
            break
    with open(DIARY_CSV, "w", newline="") as f:
        csv.writer(f).writerows(rows)


# ---------------------------------------------------------------------------- stato
def load_state():
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        s = {}
    s.setdefault("status", "idle")        # idle | open
    s.setdefault("event_id", None)
    s.setdefault("start_epoch", None)
    s.setdefault("min_z", None)           # z piu' negativo dell'evento in corso
    s.setdefault("worst_resid", None)     # resid_pct piu' negativo dell'evento
    s.setdefault("last_close_epoch", 0)
    s.setdefault("tg_offset", 0)
    s.setdefault("asks", {})              # { message_id(str): {eid, t} } domande in sospeso
    s.setdefault("annot", {})             # { eid(str): {id, inten, depth} } annotazioni Grafana
    return s


def save_state(s):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f, indent=2)
    os.replace(tmp, STATE_FILE)


def intensita_z(min_z):
    """Intensita' dall'ampiezza del calo compensato (sigma). La soglia 'media'
    coincide con Z_OPEN: ogni evento aperto e' almeno 'media'."""
    a = abs(min_z or 0.0)
    if a >= 3.0:
        return "forte"
    if a >= 1.8:
        return "media"
    return "lieve"


# --------------------------------------------------------------- gestione risposte TG
def apply_natura(state, ntoken, eid, t_label, text):
    low = text.strip().lower()
    ann = state.get("annot", {}).get(str(eid))
    if low in ("falso", "falso positivo", "niente", "nulla", "annulla", "no"):
        csv_update(eid, natura="(falso)", note="falso positivo")
        send(ntoken, f"OK, evento delle {t_label} segnato come falso positivo. "
                     f"Mi serve per tarare, grazie.")
        if ann:
            grafana_update(ann["id"], f"(falso) {ann['inten']} -{ann['depth']}%",
                           ["naso-evento", "falso"])
    else:
        csv_update(eid, natura=text.strip())
        send(ntoken, f"Annotato \"{text.strip()}\" per l'evento delle {t_label}.")
        if ann:
            grafana_update(ann["id"],
                           f"{text.strip()} ({ann['inten']} -{ann['depth']}%)",
                           ["naso-evento"])
    state["asks"].pop(str_key_for(state, eid), None)


def str_key_for(state, eid):
    for k, v in state["asks"].items():
        if v.get("eid") == eid:
            return k
    return None


def handle_replies(state, ntoken):
    updates = get_updates(ntoken, state["tg_offset"] + 1 if state["tg_offset"] else 0)
    for upd in updates:
        state["tg_offset"] = max(state["tg_offset"], upd.get("update_id", 0))
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            continue
        if (msg.get("from") or {}).get("id") != CHAT_ID:
            continue
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        low = text.lower().lstrip("/")
        asks = state["asks"]

        # comandi
        if low in ("start", "aiuto", "help"):
            send(ntoken,
                 "Sono il naso di casa. Quando sento un calo nell'aria ti chiedo "
                 "\"che puzza e'?\": rispondi col tipo (es. stalla, chimico) usando il "
                 "tasto Rispondi di Telegram sul mio messaggio, oppure \"falso\" se non "
                 "c'e' nulla. Puoi rispondere anche molto dopo. Comandi: stato, aiuto.")
            continue
        if low in ("stato", "status"):
            if asks:
                righe = "\n".join(f"- {v['t']} (id {v['eid']})" for v in asks.values())
                send(ntoken, f"Eventi in attesa di etichetta:\n{righe}")
            elif state["status"] == "open":
                send(ntoken, f"Evento in corso dalle {hhmm(state['start_epoch'])}.")
            else:
                send(ntoken, "Tutto tranquillo, nessun evento aperto.")
            continue

        # risposta diretta a una domanda specifica (robusta a ritardi/fuori ordine)
        rt = msg.get("reply_to_message")
        if rt and str(rt.get("message_id")) in asks:
            info = asks[str(rt["message_id"])]
            apply_natura(state, ntoken, info["eid"], info["t"], text)
            continue

        # testo sciolto: se c'e' un solo evento in sospeso lo attacco a quello
        if len(asks) == 1:
            info = next(iter(asks.values()))
            apply_natura(state, ntoken, info["eid"], info["t"], text)
        elif len(asks) == 0:
            send(ntoken, "Nessun evento da etichettare adesso. Quando sento qualcosa "
                         "te lo chiedo io.")
        else:
            send(ntoken, "Ci sono piu' eventi in attesa: usa il tasto Rispondi di "
                         "Telegram sul messaggio dell'evento che vuoi etichettare, "
                         "cosi' non sbaglio. (scrivi \"stato\" per la lista)")


# ------------------------------------------------------------------------ rilevamento
def find_onset_z(rows, trig_idx):
    """Retro-data l'inizio alla 'spalla' del calo: a ritroso dal trigger, il primo
    campione con z gia' risalito sopra Z_ONSET. Se entro ONSET_LOOKBACK_S non si
    torna alla spalla, ci si ferma al bordo della finestra. (rows: (t, z, resid))."""
    t_trig = rows[trig_idx][0]
    onset_idx = trig_idx
    j = trig_idx
    while j - 1 >= 0 and t_trig - rows[j - 1][0] <= ONSET_LOOKBACK_S:
        j -= 1
        onset_idx = j
        if rows[j][1] >= Z_ONSET:         # tornati alla spalla -> qui inizia il calo
            break
    return onset_idx


def _event_extrema(rows, start_epoch):
    """(min_z, worst_resid) sui campioni dell'evento (t >= start_epoch), o (None,None)."""
    ev = [(z, resid) for (t, z, resid) in rows if t >= start_epoch]
    if not ev:
        return None, None
    return min(z for z, _ in ev), min(r for _, r in ev)


def decide(state, rows):
    """Decisione PURA (nessun effetto collaterale): dallo stato + serie z ritorna
    l'azione ('open'/'close') o None. La usano sia il detector live sia il --replay,
    cosi' la logica e' una sola. rows: lista ordinata di (t, z, resid)."""
    if len(rows) < CONFIRM_N:
        return None
    now = rows[-1][0]
    recent = rows[-CONFIRM_N:]
    below = all(z <= Z_OPEN for (_, z, _) in recent)
    above = all(z >= Z_CLOSE for (_, z, _) in recent)
    if state["status"] == "idle":
        if below and (now - state.get("last_close_epoch", 0)) > COOLDOWN_S:
            trig_idx = len(rows) - CONFIRM_N
            return {"action": "open", "onset_idx": find_onset_z(rows, trig_idx)}
    elif state["status"] == "open":
        if above:
            return {"action": "close", "end_epoch": recent[0][0]}
    return None


def detect(state, ntoken, rows):
    if not rows:
        return
    # non agire su un indice stantio (naso-index giu'): meglio zitti che ciechi
    if time.time() - rows[-1][0] > INDEX_STALE_S:
        return
    # mentre l'evento e' aperto, tieni aggiornati i minimi ri-derivandoli dai dati
    if state["status"] == "open" and state.get("start_epoch"):
        mz, wr = _event_extrema(rows, state["start_epoch"])
        if mz is not None:
            state["min_z"], state["worst_resid"] = mz, wr

    act = decide(state, rows)
    if not act:
        return

    if act["action"] == "open":
        start_epoch = rows[act["onset_idx"]][0]
        eid = int(start_epoch)
        mz, wr = _event_extrema(rows, start_epoch)
        state.update(status="open", event_id=eid, start_epoch=start_epoch,
                     min_z=mz, worst_resid=wr)
        csv_append_open(eid, start_epoch)
        t_label = hhmm(start_epoch)
        r = send(ntoken,
                 f"Naso: sento qualcosa dalle {t_label}. Che puzza e'? "
                 f"Rispondi col tipo (es. stalla, chimico...) o \"falso\" se non "
                 f"c'e' nulla. (puoi rispondere anche dopo, col tasto Rispondi)")
        if r and r.get("ok"):
            mid = str(r["result"]["message_id"])
            state["asks"][mid] = {"eid": eid, "t": t_label}
            # tieni la mappa sotto controllo
            if len(state["asks"]) > MAX_ASKS:
                for k in list(state["asks"])[:-MAX_ASKS]:
                    state["asks"].pop(k, None)

    elif act["action"] == "close":
        end_epoch = act["end_epoch"]
        zmin = state.get("min_z") or 0.0
        depth = abs(min(0.0, state.get("worst_resid") or 0.0))   # |scarto%| compensato
        dur_min = round((end_epoch - state["start_epoch"]) / 60.0, 1)
        inten = intensita_z(zmin)
        csv_update(state["event_id"],
                   ora_fine=datetime.fromtimestamp(end_epoch, TZ).strftime("%H:%M:%S"),
                   durata_min=f"{dur_min}", profondita_pct=f"{depth:.0f}",
                   intensita=inten)
        send(ntoken, f"Evento chiuso: {hhmm(state['start_epoch'])}-{hhmm(end_epoch)}, "
                     f"durata {dur_min} min, intensita' {inten} "
                     f"(z {zmin:.1f}, -{depth:.0f}% sull'atteso).")
        # auto-annotazione Grafana (la natura, se arrivera' via TG, la aggiorna dopo)
        annid = grafana_annotate(state["start_epoch"], end_epoch,
                                 f"{inten} -{depth:.0f}%", ["naso-evento"])
        if annid:
            state["annot"][str(state["event_id"])] = {
                "id": annid, "inten": inten, "depth": round(depth)}
        state.update(status="idle", event_id=None, start_epoch=None,
                     min_z=None, worst_resid=None, last_close_epoch=end_epoch)


def replay(start_iso, stop_iso):
    """Ribatte gas_index.z nella finestra data e stampa gli eventi che il detector
    aprirebbe/chiuderebbe -- SIMULAZIONE pura: niente Telegram/CSV/Grafana/stato.
    Serve a verificare e tarare le soglie su dati reali."""
    itoken = parse_env(INFLUX_ENV, "INFLUX_READ_TOKEN")
    if not itoken:
        print("[naso] token mancante", file=sys.stderr); sys.exit(1)
    rows = influx_index_series(itoken, f"start:{start_iso}, stop:{stop_iso}")
    print(f"replay {start_iso}..{stop_iso}: {len(rows)} punti | "
          f"Z_OPEN={Z_OPEN} Z_CLOSE={Z_CLOSE} Z_ONSET={Z_ONSET} CONFIRM_N={CONFIRM_N}")
    sim = {"status": "idle", "last_close_epoch": 0, "start_epoch": None,
           "min_z": None, "worst_resid": None}
    for i in range(1, len(rows) + 1):
        prefix = rows[:i]
        if sim["status"] == "open":
            mz, wr = _event_extrema(prefix, sim["start_epoch"])
            if mz is not None:
                sim["min_z"], sim["worst_resid"] = mz, wr
        act = decide(sim, prefix)
        if not act:
            continue
        if act["action"] == "open":
            se = prefix[act["onset_idx"]][0]
            mz, wr = _event_extrema(prefix, se)
            sim.update(status="open", start_epoch=se, min_z=mz, worst_resid=wr)
            print(f"  OPEN  onset {hhmm(se)}  (trigger z={prefix[-1][1]:.2f} @ {hhmm(prefix[-1][0])})")
        elif act["action"] == "close":
            ee = act["end_epoch"]
            print(f"  CLOSE {hhmm(sim['start_epoch'])}-{hhmm(ee)}  zmin {sim['min_z']:.2f}  "
                  f"-{abs(sim['worst_resid'] or 0):.0f}%  [{intensita_z(sim['min_z'])}]  "
                  f"dur {round((ee - sim['start_epoch']) / 60.0, 1)}min")
            sim.update(status="idle", start_epoch=None, min_z=None,
                       worst_resid=None, last_close_epoch=ee)
    if sim["status"] == "open":
        print(f"  (ancora aperto a fine finestra: onset {hhmm(sim['start_epoch'])}, "
              f"zmin {sim['min_z']:.2f})")


def main():
    if len(sys.argv) >= 4 and sys.argv[1] == "--replay":
        replay(sys.argv[2], sys.argv[3])
        return
    ntoken = parse_env(NASO_ENV, "NASO_BOT_TOKEN")
    itoken = parse_env(INFLUX_ENV, "INFLUX_READ_TOKEN")
    if not ntoken or not itoken:
        print("[naso] token mancanti", file=sys.stderr)
        sys.exit(1)
    ensure_csv()
    state = load_state()
    # 1) leggi eventuali risposte/etichette (anche tardive, fuori ordine)
    handle_replies(state, ntoken)
    # 2) rileva sul residuo compensato gas_index.z
    try:
        rows = influx_index_series(itoken)
        detect(state, ntoken, rows)
    except Exception as e:
        print(f"[naso] detect errore: {e}", file=sys.stderr)
    # 3) specchia il diario nella misura `evento` (per la tabella Grafana)
    wtoken = parse_env(INFLUX_ENV, "INFLUX_TOKEN")
    if wtoken:
        mirror_csv_to_influx(wtoken)
    save_state(state)


if __name__ == "__main__":
    main()
