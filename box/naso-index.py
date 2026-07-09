#!/usr/bin/env python3
"""
Snoopy-Naso - Indice "Aria Compensata".

La resistenza gas del BME680 dipende fortemente da temperatura e umidita' (e dal
ciclo diurno per isteresi termica): all'aperto oscilla ~3x al giorno PER MOTIVI
FISICI, non per qualita' dell'aria. Una soglia assoluta sul gas grezzo e' quindi
inservibile (segnerebbe ATTENZIONE ogni pomeriggio caldo e mai di notte fredda).

Questo script "scompone" temp+umidita'+ora-del-giorno dal gas con una regressione
log-lineare rifittata su finestra mobile (ultimi N giorni -> segue da sola la
deriva lenta del sensore), e pubblica il RESIDUO come indice stabile giorno/notte:
  - residuo ~ 0  -> aria come prevista (normale)
  - residuo < 0  -> gas sotto il previsto = piu' VOC = "peggio"
Scrive la misura `gas_index` in InfluxDB (campi: idx 0-100, z in sigma, resid_pct,
gas grezzo, gas atteso, bias_pct). R^2 tipico ~0.98 (vedi tuning 03/06).

Modello: ln(gas) = b0 + b1*temp + b2*umid [+ armoniche 24h/12h se c'e' >=18h storia].
Sigma robusta (MAD) per non farsi gonfiare dagli eventi. Stateless: rifitta ogni run.

DE-BIAS (dal 2026-07-09): quando il sensore deriva, il fit a finestra 7gg insegue
in ritardo e il residuo resta cronicamente scentrato (a giugno: media z -1.34,
il 64% del tempo sotto Z_CLOSE -> eventi che non chiudevano mai; a inizio luglio
bias opposto +2/+3 sigma -> eventi reali mascherati). Fix in due pezzi, validato
in backtest su giugno (deriva forte) e luglio (stabile):
  1. termine LINEARE DI DERIVA nel modello OLS (DRIFT_TERM) - modella la
     componente monotona senza il lag che avrebbe una mediana inseguitrice;
  2. il residuo viene comunque ri-centrato sulla MEDIANA MOBILE LENTA delle
     ultime DEBIAS_H ore (raccoglie cio' che la retta non spiega). 48h e non
     24h: un evento lungo 11h occupa il 46% di una finestra 24h (la mediana
     lo inseguirebbe) ma solo il 23% di una 48h.
La sigma robusta e' calcolata sui residui gia' de-biasati.
Risultato backtest: media z -1.34 -> -0.01 (giu) / -0.15 -> +0.03 (lug),
eventi-mostro >12h di giugno spariti, nessun evento reale di luglio perso.
"""
import csv, io, math, sys, urllib.request, urllib.parse
from bisect import insort, bisect_left
from collections import deque
from datetime import datetime, timezone
from statistics import median

# --- percorsi / config ---
INFLUX_ENV = "/home/mak/esp-naso/.env"
INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "snoopy-lab"
BUCKET     = "aria"
MEAS       = "gas_index"

WINDOW           = "7d"      # finestra mobile di fit (segue la deriva del sensore)
AGG              = "5m"      # risoluzione di aggregazione per il fit
MIN_POINTS       = 24        # troppo pochi punti -> non fittare
HARMONICS_MIN_H  = 18.0      # ore di storia minime per attivare le armoniche diurne
Z_FULL_RED       = 6.0       # z=-6 -> idx 0 ; z=0 -> idx 100 (mappa lineare)
DEBIAS_H         = 48.0      # finestra della mediana mobile lenta di de-bias
DEBIAS_MIN_H     = 12.0      # storia minima perche' il de-bias di un punto sia definito
DRIFT_TERM       = True      # termine lineare di deriva nel modello OLS


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
    s = s.strip().replace("Z", "+00:00")
    # taglia eventuali nanosecondi (fromisoformat regge max microsecondi)
    if "." in s:
        head, rest = s.split(".", 1)
        frac = ""
        for ch in rest:
            if ch.isdigit():
                frac += ch
            else:
                rest_tz = rest[len(frac):]
                break
        else:
            rest_tz = ""
        s = head + "." + frac[:6] + rest_tz
    return datetime.fromisoformat(s).timestamp()


def influx_joined(token):
    """Ritorna lista di (epoch, gas, temp, umid) a passo AGG, righe complete."""
    flux = (f'from(bucket:"{BUCKET}") '
            f'|> range(start:-{WINDOW}) '
            f'|> filter(fn:(r)=> r._measurement=="aria" and '
            f'(r.sensor=="temperatura" or r.sensor=="umidita" or r.sensor=="resistenza_gas")) '
            f'|> aggregateWindow(every:{AGG},fn:mean,createEmpty:false) '
            f'|> group() '
            f'|> pivot(rowKey:["_time"],columnKey:["sensor"],valueColumn:"_value") '
            f'|> keep(columns:["_time","temperatura","umidita","resistenza_gas"])')
    url = f"{INFLUX_URL}/api/v2/query?org={urllib.parse.quote(INFLUX_ORG)}"
    req = urllib.request.Request(url, data=flux.encode(),
                                 headers={"Authorization": f"Token {token}",
                                          "Content-Type": "application/vnd.flux",
                                          "Accept": "application/csv"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode()
    return parse_joined_csv(text)


def parse_joined_csv(text):
    rows, hdr = [], None
    ti = gi = tei = hi = None
    for row in csv.reader(io.StringIO(text)):
        if not row or row[0].startswith('#'):
            continue
        if hdr is None:
            hdr = row
            try:
                ti = hdr.index("_time"); gi = hdr.index("resistenza_gas")
                tei = hdr.index("temperatura"); hi = hdr.index("umidita")
            except ValueError:
                return []
            continue
        mx = max(ti, gi, tei, hi)
        if len(row) <= mx or not row[ti] or not row[gi] or not row[tei] or not row[hi]:
            continue
        try:
            rows.append((rfc3339_epoch(row[ti]), float(row[gi]),
                         float(row[tei]), float(row[hi])))
        except ValueError:
            continue
    rows.sort()
    return rows


def solve(A, c):
    """Risolve A x = c (Gauss con pivoting parziale). A: nxn, c: n."""
    n = len(c)
    M = [A[i][:] + [c[i]] for i in range(n)]
    for i in range(n):
        p = max(range(i, n), key=lambda r: abs(M[r][i]))
        if abs(M[p][i]) < 1e-12:
            return None
        M[i], M[p] = M[p], M[i]
        piv = M[i][i]
        for k in range(i, n + 1):
            M[i][k] /= piv
        for j in range(n):
            if j != i and M[j][i]:
                f = M[j][i]
                for k in range(i, n + 1):
                    M[j][k] -= f * M[i][k]
    return [M[i][n] for i in range(n)]


def make_feats(use_harm, use_drift, t0):
    W = 2 * math.pi / 24.0
    def f(ep, te, hu):
        x = [1.0, te, hu]
        if use_drift:
            x.append((ep - t0) / 86400.0)
        if use_harm:
            h = hour_of(ep)
            x += [math.sin(W * h), math.cos(W * h),
                  math.sin(2 * W * h), math.cos(2 * W * h)]
        return x
    return f


def hour_of(epoch):
    # ora locale (Europe/Rome) come usata nel fit del tuning; va bene anche UTC
    # purche' coerente tra fit e predizione -> usiamo l'ora UTC del timestamp
    dt = datetime.fromtimestamp(epoch, timezone.utc)
    return dt.hour + dt.minute / 60.0


def rolling_median(times, values, window_s, min_history_s):
    """Mediana mobile trailing su finestra temporale.

    Ritorna una lista parallela a values: out[i] = mediana dei values[j] con
    times[j] in (times[i]-window_s, times[i]]; None finche' il punto non ha
    almeno min_history_s di storia alle spalle (mediana non affidabile).
    """
    out = [None] * len(values)
    buf = []                 # valori in finestra, ordinati
    q = deque()              # (time, value) in ordine di arrivo
    t0 = times[0] if times else 0
    for i, (t, v) in enumerate(zip(times, values)):
        insort(buf, v)
        q.append((t, v))
        while q and q[0][0] <= t - window_s:
            _, old = q.popleft()
            del buf[bisect_left(buf, old)]
        if t - t0 >= min_history_s:
            m = len(buf)
            out[i] = buf[m // 2] if m % 2 else 0.5 * (buf[m // 2 - 1] + buf[m // 2])
    return out


def fit_and_index(rows):
    span_h = (rows[-1][0] - rows[0][0]) / 3600.0
    use_harm = span_h >= HARMONICS_MIN_H
    use_drift = DRIFT_TERM and span_h >= HARMONICS_MIN_H
    feats = make_feats(use_harm, use_drift, rows[0][0])
    n = len(feats(rows[0][0], 0, 0))
    A = [[0.0] * n for _ in range(n)]
    c = [0.0] * n
    for ep, g, te, hu in rows:
        x = feats(ep, te, hu); y = math.log(g)
        for i in range(n):
            c[i] += x[i] * y
            for j in range(n):
                A[i][j] += x[i] * x[j]
    b = solve(A, c)
    if b is None:
        raise RuntimeError("fit singolare")

    def pred(ep, te, hu):
        x = feats(ep, te, hu)
        return sum(b[i] * x[i] for i in range(n))

    resid = [math.log(g) - pred(ep, te, hu) for ep, g, te, hu in rows]
    # R^2
    ybar = sum(math.log(g) for _, g, _, _ in rows) / len(rows)
    sstot = sum((math.log(g) - ybar) ** 2 for _, g, _, _ in rows) or 1e-9
    ssres = sum(r * r for r in resid)
    r2 = 1 - ssres / sstot
    # de-bias: livello lento del residuo (mediana mobile DEBIAS_H)
    times = [ep for ep, _, _, _ in rows]
    biases = rolling_median(times, resid, DEBIAS_H * 3600.0, DEBIAS_MIN_H * 3600.0)
    med_all = median(resid)
    debiased = [r - bm for r, bm in zip(resid, biases) if bm is not None]
    # sigma robusta (MAD) sui residui de-biasati (fallback: residui grezzi)
    base = debiased if len(debiased) >= MIN_POINTS else resid
    med = median(base)
    mad = median([abs(r - med) for r in base]) or 1e-9
    sigma = 1.4826 * mad
    # punto corrente = ultima riga
    ep, g, te, hu = rows[-1]
    bias = biases[-1] if biases and biases[-1] is not None else med_all
    cur = math.log(g) - pred(ep, te, hu) - bias
    z = cur / sigma
    resid_pct = (math.exp(cur) - 1) * 100.0
    idx = max(0.0, min(100.0, 100.0 + min(z, 0.0) * (100.0 / Z_FULL_RED)))
    return {
        "epoch": ep, "gas": g,
        "gas_expected": math.exp(pred(ep, te, hu) + bias),
        "resid_pct": resid_pct, "z": z, "idx": idx,
        "bias_pct": (math.exp(bias) - 1) * 100.0,
        "r2": r2, "sigma": sigma, "n": len(rows), "span_h": span_h, "harm": use_harm,
    }


def influx_write(token, res):
    line = (f'{MEAS} idx={res["idx"]:.1f},z={res["z"]:.4f},'
            f'resid_pct={res["resid_pct"]:.3f},gas={res["gas"]:.1f},'
            f'gas_expected={res["gas_expected"]:.1f},'
            f'bias_pct={res["bias_pct"]:.3f},r2={res["r2"]:.4f} '
            f'{int(res["epoch"])}')
    url = (f"{INFLUX_URL}/api/v2/write?org={urllib.parse.quote(INFLUX_ORG)}"
           f"&bucket={urllib.parse.quote(BUCKET)}&precision=s")
    req = urllib.request.Request(url, data=line.encode(),
                                 headers={"Authorization": f"Token {token}",
                                          "Content-Type": "text/plain; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.status


def main():
    wtoken = parse_env(INFLUX_ENV, "INFLUX_TOKEN")        # write
    rtoken = parse_env(INFLUX_ENV, "INFLUX_READ_TOKEN")   # read
    if not wtoken or not rtoken:
        print("[idx] token mancanti", file=sys.stderr); sys.exit(1)
    try:
        rows = influx_joined(rtoken)
    except Exception as e:
        print(f"[idx] query errore: {e}", file=sys.stderr); sys.exit(1)
    if len(rows) < MIN_POINTS:
        print(f"[idx] storia insufficiente ({len(rows)} punti) - skip", file=sys.stderr)
        return
    try:
        res = fit_and_index(rows)
    except Exception as e:
        print(f"[idx] fit errore: {e}", file=sys.stderr); sys.exit(1)
    if "--dry" in sys.argv:
        import json
        print(json.dumps({k: (round(v, 3) if isinstance(v, float) else v)
                          for k, v in res.items()}, indent=2))
        return
    try:
        st = influx_write(wtoken, res)
        print(f"[idx] scritto idx={res['idx']:.0f} z={res['z']:+.2f} "
              f"resid={res['resid_pct']:+.1f}% bias={res['bias_pct']:+.1f}% "
              f"r2={res['r2']:.3f} n={res['n']} span={res['span_h']:.0f}h "
              f"harm={res['harm']} http={st}")
    except Exception as e:
        print(f"[idx] write errore: {e}", file=sys.stderr); sys.exit(1)


if __name__ == "__main__":
    main()
