import sys
import json
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.platypus.flowables import Flowable


# ===============================
# ARGS
# ===============================
def _parse_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


eur_per_hour = _parse_float(sys.argv[1], 0.0) if len(sys.argv) > 1 else 0.0
output_pdf_name = sys.argv[2] if len(sys.argv) > 2 else "process_report.pdf"

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
ASSETS_DIR = BASE_DIR / "assets"
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / output_pdf_name

LAST_METRICS_PATH = UPLOAD_DIR / "last_metrics.json"
PREV_METRICS_PATH = UPLOAD_DIR / "previous_metrics.json"

# ✅ NEW: history for trends
HISTORY_PATH = DATA_DIR / "metrics_history.json"

LOGO_PATH = ASSETS_DIR / "logo.png"  # mag ontbreken


# ===============================
# HELPERS
# ===============================
def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_roll_metrics():
    """
    last_metrics.json -> previous_metrics.json (overwrite)
    """
    if LAST_METRICS_PATH.exists():
        try:
            shutil.copyfile(LAST_METRICS_PATH, PREV_METRICS_PATH)
        except Exception:
            pass


def _pct_change(curr, prev):
    """
    Returns percent change, or None if not computable
    """
    try:
        curr = float(curr)
        prev = float(prev)
        if prev == 0:
            return None
        return (curr - prev) / prev * 100.0
    except Exception:
        return None


def _format_eur(x):
    try:
        return f"€{float(x):,.0f}".replace(",", ".")
    except Exception:
        return "€0"


def _format_hours(x):
    try:
        return f"{float(x):,.1f} uur".replace(",", ".")
    except Exception:
        return "0.0 uur"


def _format_fte(x):
    try:
        return f"{float(x):.2f} FTE"
    except Exception:
        return "0.00 FTE"


def _format_pct(x):
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except Exception:
        return "0,0%"


def _load_history():
    data = _read_json(HISTORY_PATH)
    return data if isinstance(data, list) else []


def _save_history(items: list):
    _write_json(HISTORY_PATH, items)


# ===============================
# CSV INLEZEN + KOLOM NORMALISATIE
# ===============================
if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")

df = pd.read_csv(CSV_PATH)

COLUMN_ALIASES = {
    "case_id": ["case_id", "case", "caseid", "ticket_id", "order_id"],
    "timestamp": ["timestamp", "time", "datetime", "date"],
    "event": ["event", "activity", "step", "status", "action", "event_name"],
}

normalized = {}
for canonical, options in COLUMN_ALIASES.items():
    for col in options:
        if col in df.columns:
            normalized[canonical] = col
            break

missing = set(COLUMN_ALIASES.keys()) - set(normalized.keys())
if missing:
    raise ValueError(f"Ontbrekende kolommen: {missing}. Gevonden: {list(df.columns)}")

df = df.rename(columns={v: k for k, v in normalized.items()})


# ===============================
# CLEANUP + SORT
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# PERIODE (voor extrapolatie)
# ===============================
period_start = df["timestamp"].min()
period_end = df["timestamp"].max()
period_hours = 0.0
if pd.notna(period_start) and pd.notna(period_end):
    period_hours = (period_end - period_start).total_seconds() / 3600.0

MIN_PERIOD_HOURS = 1.0
can_extrapolate = period_hours >= MIN_PERIOD_HOURS


# ===============================
# DUUR PER STAP
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (df["next_timestamp"] - df["timestamp"]).dt.total_seconds() / 3600.0

df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]


# ===============================
# BASELINE + DELAYS
# ===============================
baseline = df.groupby("event")["duration_hours"].median().rename("baseline_hours")
df = df.join(baseline, on="event")

df["is_delay"] = df["duration_hours"] > 1.5 * df["baseline_hours"]
df["impact_hours"] = (df["duration_hours"] - df["baseline_hours"]).clip(lower=0)

delays = df[df["is_delay"]].copy()
delays["impact_eur"] = delays["impact_hours"] * eur_per_hour if eur_per_hour > 0 else 0.0

summary = (
    delays.groupby("event")
    .agg(
        occurrences=("impact_hours", "count"),
        total_impact_hours=("impact_hours", "sum"),
        avg_impact_hours=("impact_hours", "mean"),
        total_impact_eur=("impact_eur", "sum"),
    )
    .sort_values("total_impact_hours", ascending=False)
    .reset_index()
)

total_impact_hours = float(delays["impact_hours"].sum()) if not delays.empty else 0.0
total_impact_eur = float(delays["impact_eur"].sum()) if not delays.empty else 0.0


# ===============================
# MANAGEMENT METRICS (maand/jaar + FTE + besparing)
# ===============================
MONTH_HOURS = 30 * 24  # 30 dagen
FTE_HOURS_PER_MONTH = 160.0

monthly_factor = (MONTH_HOURS / period_hours) if can_extrapolate else 0.0
monthly_hours_est = total_impact_hours * monthly_factor if can_extrapolate else 0.0
monthly_eur_est = total_impact_eur * monthly_factor if (can_extrapolate and eur_per_hour > 0) else 0.0

yearly_hours_est = monthly_hours_est * 12 if can_extrapolate else 0.0
yearly_eur_est = monthly_eur_est * 12 if can_extrapolate else 0.0

fte_equivalent = (monthly_hours_est / FTE_HOURS_PER_MONTH) if can_extrapolate else 0.0

improve_pct = 0.20
potential_saving_hours = monthly_hours_est * improve_pct if can_extrapolate else 0.0
potential_saving_eur = monthly_eur_est * improve_pct if can_extrapolate and eur_per_hour > 0 else 0.0


# ===============================
# TOP BOTTLENECK
# ===============================
top_bottleneck_event = None
top_bottleneck_hours = 0.0
if not summary.empty:
    top_bottleneck_event = str(summary.iloc[0]["event"])
    top_bottleneck_hours = float(summary.iloc[0]["total_impact_hours"])


# ===============================
# METRICS ROLL + SAVE (bestaand)
# ===============================
_safe_roll_metrics()
previous_metrics = _read_json(PREV_METRICS_PATH)

current_metrics = {
    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "period": {
        "start": period_start.isoformat() if pd.notna(period_start) else None,
        "end": period_end.isoformat() if pd.notna(period_end) else None,
        "hours": period_hours,
    },
    "impact": {
        "total_hours": total_impact_hours,
        "total_eur": total_impact_eur,
        "monthly_hours_est": monthly_hours_est,
        "monthly_eur_est": monthly_eur_est,
        "yearly_hours_est": yearly_hours_est,
        "yearly_eur_est": yearly_eur_est,
        "fte_equivalent": fte_equivalent,
        "potential_saving_hours": potential_saving_hours,
        "potential_saving_eur": potential_saving_eur,
    },
    "top_bottleneck": {
        "event": top_bottleneck_event,
        "impact_hours": top_bottleneck_hours,
    },
    "pdf": OUTPUT_PDF.name,

    # ✅ NEW: top-level shortcuts for UI (app.py reads these)
    "monthly_eur_est": monthly_eur_est,
    "yearly_eur_est": yearly_eur_est,
}


# ===============================
# VERGELIJKING BEREKENEN (bestaand)
# ===============================
comparison = None
if previous_metrics and isinstance(previous_metrics, dict):
    prev_imp = previous_metrics.get("impact", {}) or {}
    curr_imp = current_metrics.get("impact", {}) or {}

    prev_total_eur = prev_imp.get("total_eur", 0.0)
    curr_total_eur = curr_imp.get("total_eur", 0.0)

    prev_month_eur = prev_imp.get("monthly_eur_est", 0.0)
    curr_month_eur = curr_imp.get("monthly_eur_est", 0.0)

    prev_fte = prev_imp.get("fte_equivalent", 0.0)
    curr_fte = curr_imp.get("fte_equivalent", 0.0)

    pct_total = _pct_change(curr_total_eur, prev_total_eur) if eur_per_hour > 0 else _pct_change(
        curr_imp.get("total_hours", 0.0), prev_imp.get("total_hours", 0.0)
    )
    delta_month_eur = (curr_month_eur - prev_month_eur) if eur_per_hour > 0 else None
    delta_fte = (curr_fte - prev_fte) if can_extrapolate else None

    prev_top = (previous_metrics.get("top_bottleneck", {}) or {}).get("event")
    curr_top = (current_metrics.get("top_bottleneck", {}) or {}).get("event")

    comparison = {
        "pct_total": pct_total,
        "delta_month_eur": delta_month_eur,
        "delta_fte": delta_fte,
        "prev_top": prev_top,
        "curr_top": curr_top,
    }


# ===============================
# ADVIESLOGICA (bestaand)
# ===============================
ADVICE_MAP = {
    "assigned": "Overweeg automatische ticket-toewijzing en een SLA op eerste reactie.",
    "waiting": "Introduceer klant-reminders en pauzeer SLA bij wachten op klant.",
    "response": "Analyseer agentbelasting en stel WIP-limieten in.",
    "triage": "Versnel triage met vaste categorieën en prioriteitsregels.",
    "created": "Standaardiseer intake en automatiseer ticketcreatie waar mogelijk.",
}

def generate_advice(events):
    items = []
    for ev in events:
        key = str(ev).lower()
        chosen = None
        for k, text in ADVICE_MAP.items():
            if k in key:
                chosen = text
                break
        if not chosen:
            chosen = "Analyseer deze stap op standaardisatie, automatisering en duidelijke ownership."
        items.append((str(ev), chosen))
    return items

top_events_for_advice = summary.head(3)["event"].tolist() if not summary.empty else []
advice_items = generate_advice(top_events_for_advice)


# ===============================
# ✅ NEW: SLA INTELLIGENCE (per type) + trends + upgrade signals + NL copy
# ===============================
def map_sla_type(event: str) -> str:
    s = (str(event) if event is not None else "").lower()
    if "waiting" in s:
        return "waiting"
    if "resolved" in s or "closed" in s:
        return "resolution"
    # alles wat start/assign/response/triage raakt -> first_response
    if "assigned" in s or "created" in s or "response" in s or "triage" in s:
        return "first_response"
    return "other"

# SLA breach (baseline-based) — we laten je bestaande baseline intact
df["sla_type"] = df["event"].astype(str).apply(map_sla_type)
df["sla_breach"] = df["duration_hours"] > (df["baseline_hours"] * 1.2)

sla_by_type = {}
for t in ["first_response", "waiting", "resolution"]:
    sub = df[df["sla_type"] == t]
    if sub.empty:
        continue

    steps = int(len(sub))
    breaches = int(sub["sla_breach"].sum())
    compliance_pct = 100.0 * (steps - breaches) / steps if steps > 0 else 0.0

    # risico: overschrijdings-uren * eur_per_hour, geëxtrapoleerd naar maand
    breach_hours = float(sub.loc[sub["sla_breach"], "impact_hours"].sum()) if eur_per_hour > 0 else 0.0
    risk_period_eur = breach_hours * eur_per_hour if eur_per_hour > 0 else 0.0
    monthly_risk_eur_est = risk_period_eur * monthly_factor if (eur_per_hour > 0 and can_extrapolate) else risk_period_eur

    sla_by_type[t] = {
        "steps": steps,
        "breaches": breaches,
        "compliance_pct": round(compliance_pct, 1),
        "monthly_risk_eur_est": round(monthly_risk_eur_est, 0),
    }

# history append (per run)
history = _load_history()
history.append({
    "generated_at": current_metrics["generated_at"],
    "period": current_metrics["period"],
    "sla_by_type": sla_by_type,
})
_save_history(history)

# trend by type (last vs previous)
sla_trend_by_type = {}
if len(history) >= 2:
    prev = history[-2].get("sla_by_type") or {}
    curr = history[-1].get("sla_by_type") or {}
    for t, v in curr.items():
        if t not in prev:
            continue
        sla_trend_by_type[t] = {
            "compliance_delta_pp": round(float(v.get("compliance_pct", 0.0)) - float(prev[t].get("compliance_pct", 0.0)), 1),
            "risk_delta_eur": round(float(v.get("monthly_risk_eur_est", 0.0)) - float(prev[t].get("monthly_risk_eur_est", 0.0)), 0),
        }

# Upgrade signals (NL, feitelijk)
upgrade_signals = []
for t, v in sla_by_type.items():
    comp = float(v.get("compliance_pct", 0.0) or 0.0)
    risk = float(v.get("monthly_risk_eur_est", 0.0) or 0.0)

    if comp < 90.0:
        upgrade_signals.append({
            "type": "lage_compliance",
            "severity": "high" if comp < 80 else "medium",
            "sla_type": t,
            "message": f"De SLA-compliance voor {t.replace('_',' ')} ligt op {comp:.1f}%, onder de aanbevolen ondergrens van 90%.",
        })

    if risk >= 1000.0:
        upgrade_signals.append({
            "type": "financieel_risico",
            "severity": "high" if risk >= 10000 else "medium",
            "sla_type": t,
            "message": f"De geschatte financiële impact van {t.replace('_',' ')} bedraagt circa {_format_eur(risk)} per maand.",
        })

# trend signal: dalend over meerdere metingen (2 dalingen op rij)
if len(history) >= 3:
    last3 = history[-3:]
    for t in sla_by_type.keys():
        comps = []
        for h in last3:
            vv = (h.get("sla_by_type") or {}).get(t)
            comps.append(float(vv.get("compliance_pct", 0.0)) if vv else None)
        if None not in comps and comps[0] > comps[1] > comps[2]:
            upgrade_signals.append({
                "type": "negatieve_trend",
                "severity": "high",
                "sla_type": t,
                "message": f"De SLA-compliance voor {t.replace('_',' ')} vertoont een dalende trend over meerdere metingen.",
            })

# AI-advies (NL, data-first)
ai_advice = []
# sorteer op risico desc
ranking = sorted(
    [(t, float(v.get("monthly_risk_eur_est", 0.0) or 0.0)) for t, v in sla_by_type.items()],
    key=lambda x: x[1],
    reverse=True
)
for t, risk in ranking:
    if risk <= 0:
        continue
    if t == "first_response":
        ai_advice.append({
            "sla_type": t,
            "title": "Versnel eerste reactie",
            "summary": "Door optimalisatie van intake en automatische toewijzing kan de eerste reactietijd structureel worden verkort.",
            "monthly_risk_reduction_est": round(risk * 0.25, 0),
            "actions": [
                "Stel een SLA in op eerste reactie (< 2 uur).",
                "Activeer automatische tickettoewijzing.",
                "Beperk het aantal gelijktijdige open tickets per agent (WIP-limieten).",
            ],
        })
    elif t == "waiting":
        ai_advice.append({
            "sla_type": t,
            "title": "Beperk wachttijd bij klant",
            "summary": "Langdurige wachttijden bij klanten veroorzaken structureel capaciteitsverlies.",
            "monthly_risk_reduction_est": round(risk * 0.40, 0),
            "actions": [
                "Pauzeer SLA bij wachten op klant (contractueel vastleggen).",
                "Stuur automatische herinneringen na 24 en 48 uur.",
                "Sluit inactieve tickets automatisch na waarschuwing.",
            ],
        })
    elif t == "resolution":
        ai_advice.append({
            "sla_type": t,
            "title": "Verkort oplostijd",
            "summary": "Door betere escalatie en ownership kan de oplostijd structureel worden verkort.",
            "monthly_risk_reduction_est": round(risk * 0.30, 0),
            "actions": [
                "Introduceer escalatieregels na 24 uur.",
                "Wijs expliciete ownership toe per categorie.",
                "Analyseer herhaalproblemen en maak structurele fixes.",
            ],
        })
    if len(ai_advice) >= 3:
        break

# attach to metrics (no breaking)
current_metrics["sla_by_type"] = sla_by_type
current_metrics["sla_trend_by_type"] = sla_trend_by_type
current_metrics["upgrade_signals"] = upgrade_signals[:5]
current_metrics["ai_advice"] = ai_advice

# save metrics where app.py expects them
_write_json(LAST_METRICS_PATH, current_metrics)


# ===============================
# VISUALISATIE (bestaand + uitbreiding)
# ===============================
class DrawingFlowable(Flowable):
    def __init__(self, drawing: Drawing):
        super().__init__()
        self.drawing = drawing
        self.width = drawing.width
        self.height = drawing.height

    def draw(self):
        from reportlab.graphics import renderPDF
        renderPDF.draw(self.drawing, self.canv, 0, 0)


def make_bar_chart(data, title, width=520, height=360):
    """
    data: list of (label, value) in hours
    """
    d = Drawing(width, height)
    d.add(String(0, height - 16, title, fontName="Helvetica-Bold", fontSize=13, fillColor=colors.HexColor("#0f172a")))

    if not data:
        d.add(String(0, height - 40, "Geen data beschikbaar.", fontName="Helvetica", fontSize=10))
        return d

    max_val = max(v for _, v in data) if data else 1.0
    if max_val <= 0:
        max_val = 1.0

    left_label = 190
    right_pad = 60
    chart_w = width - left_label - right_pad
    top = height - 44
    row_h = max(24, int((top - 10) / len(data)))

    y = top - row_h
    for label, val in data:
        bar_w = (float(val) / max_val) * chart_w
        d.add(String(0, y + 7, str(label)[:30], fontName="Helvetica", fontSize=9))
        d.add(Rect(left_label, y + 4, bar_w, 12, fillColor=colors.HexColor("#0f172a"), strokeColor=None))
        d.add(String(left_label + chart_w + 6, y + 7, f"{float(val):.1f}u", fontName="Helvetica", fontSize=9))
        y -= row_h
        if y < 8:
            break

    return d


# ✅ NEW: SLA trend line chart (simple)
def make_line_chart(points, title, width=520, height=260, suffix="", fmt="{:.1f}"):
    d = Drawing(width, height)
    d.add(String(0, height - 16, title, fontName="Helvetica-Bold", fontSize=13, fillColor=colors.HexColor("#0f172a")))

    if len(points) < 2:
        d.add(String(0, height - 40, "Nog onvoldoende data voor trendgrafiek.", fontName="Helvetica", fontSize=10))
        return d

    vals = [float(v) for _, v in points]
    min_v, max_v = min(vals), max(vals)
    if min_v == max_v:
        max_v += 1.0

    left, bottom = 60, 40
    right, top = width - 20, height - 50
    step = (right - left) / (len(points) - 1)

    prev = None
    for i, (label, val) in enumerate(points):
        val = float(val)
        x = left + i * step
        y = bottom + (val - min_v) / (max_v - min_v) * (top - bottom)

        d.add(String(x - 10, bottom - 15, label, fontName="Helvetica", fontSize=8))
        d.add(String(x - 10, y + 6, fmt.format(val) + suffix, fontName="Helvetica", fontSize=8))

        if prev:
            d.add(Rect(prev[0], prev[1], x - prev[0], 1.5, fillColor=colors.HexColor("#2563eb"), strokeColor=None))
        prev = (x, y)

    return d


# ===============================
# PDF HEADER/FOOTER (bestaand)
# ===============================
def header_footer(canvas, doc):
    canvas.saveState()

    if LOGO_PATH.exists():
        try:
            canvas.drawImage(
                str(LOGO_PATH),
                36,
                A4[1] - 50,
                width=120,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception:
            pass

    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(colors.grey)
    canvas.drawString(36, 28, f"Prolixia • {datetime.now().strftime('%d-%m-%Y')}")
    canvas.drawRightString(A4[0] - 36, 28, f"Pagina {doc.page}")

    canvas.restoreState()


# ===============================
# PDF GENERATIE (bestaand + toevoegingen)
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(
    str(OUTPUT_PDF),
    pagesize=A4,
    rightMargin=36,
    leftMargin=36,
    topMargin=72,
    bottomMargin=36,
)

elements = []

# Titel
elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 10))

# Periode
if pd.notna(period_start) and pd.notna(period_end):
    elements.append(Paragraph(
        f"<b>Analyseperiode:</b> {period_start.strftime('%d-%m-%Y %H:%M')} t/m {period_end.strftime('%d-%m-%Y %H:%M')}",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 6))

elements.append(Paragraph(
    f"<b>Totale impact (delays vs baseline):</b> {_format_hours(total_impact_hours)}"
    + (f" (≈ {_format_eur(total_impact_eur)})" if eur_per_hour > 0 else ""),
    styles["Normal"]
))
elements.append(Spacer(1, 14))

# Managementsamenvatting (bestaand)
elements.append(Paragraph("<b>Managementsamenvatting</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if can_extrapolate:
    elements.append(Paragraph(
        f"• Geschatte maandimpact: <b>{_format_hours(monthly_hours_est)}</b>"
        + (f" (≈ <b>{_format_eur(monthly_eur_est)}</b>)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        f"• FTE-equivalent: <b>{_format_fte(fte_equivalent)}</b> (op basis van 160 uur/maand)",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        f"• Jaarimpact: <b>{_format_hours(yearly_hours_est)}</b>"
        + (f" (≈ <b>{_format_eur(yearly_eur_est)}</b>)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph(
        f"• Potentiële besparing bij 20% verbetering: <b>{_format_hours(potential_saving_hours)}/maand</b>"
        + (f" (≈ <b>{_format_eur(potential_saving_eur)}</b>/maand)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
else:
    elements.append(Paragraph(
        "• Extrapolatie naar maand/jaar niet mogelijk (analyseperiode te klein of onduidelijk).",
        styles["Normal"]
    ))

elements.append(Spacer(1, 14))

# Vergelijking vorige periode (bestaand)
elements.append(Paragraph("<b>Vergelijking met vorige periode</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if comparison is None:
    elements.append(Paragraph(
        "ℹ️ Dit is de <b>eerste analyse</b>. De volgende analyse wordt automatisch vergeleken met deze nulmeting.",
        styles["Normal"]
    ))
else:
    pct = comparison.get("pct_total", None)
    delta_month = comparison.get("delta_month_eur", None)
    delta_fte = comparison.get("delta_fte", None)

    if pct is not None:
        trend_txt = "📉 Verbetering" if pct < 0 else ("📈 Verslechtering" if pct > 0 else "➖ Geen verandering")
        elements.append(Paragraph(f"{trend_txt} t.o.v. vorige periode: <b>{pct:+.1f}%</b>", styles["Normal"]))
        elements.append(Spacer(1, 6))

    if eur_per_hour > 0 and delta_month is not None:
        elements.append(Paragraph(
            f"• Maandimpact verschil: <b>{_format_eur(delta_month)}</b> "
            f"({'besparing' if delta_month < 0 else 'extra kosten' if delta_month > 0 else 'gelijk'})",
            styles["Normal"]
        ))
        elements.append(Spacer(1, 4))

    if can_extrapolate and delta_fte is not None:
        elements.append(Paragraph(
            f"• FTE verschil: <b>{delta_fte:+.2f} FTE</b>",
            styles["Normal"]
        ))
        elements.append(Spacer(1, 4))

    prev_top = comparison.get("prev_top")
    curr_top = comparison.get("curr_top")
    if curr_top:
        if prev_top and prev_top != curr_top:
            elements.append(Paragraph(
                f"• Grootste bottleneck is verschoven van <b>{prev_top}</b> → <b>{curr_top}</b>",
                styles["Normal"]
            ))
        else:
            elements.append(Paragraph(
                f"• Grootste bottleneck blijft: <b>{curr_top}</b>",
                styles["Normal"]
            ))

elements.append(Spacer(1, 16))

# ✅ NEW: SLA Intelligence sectie (NL copy)
elements.append(Paragraph("<b>SLA Intelligence per processtap</b>", styles["Heading2"]))
elements.append(Spacer(1, 6))
elements.append(Paragraph(
    "Dit overzicht toont per SLA-type de mate van naleving en de bijbehorende financiële impact.",
    styles["Normal"]
))
elements.append(Spacer(1, 8))

if not sla_by_type:
    elements.append(Paragraph("Geen SLA-type data beschikbaar.", styles["Normal"]))
else:
    table_data = [["SLA-type", "Steps", "Breaches", "Compliance", "Risico/maand (€)"]]
    for t, v in sla_by_type.items():
        table_data.append([
            t.replace("_", " ").title(),
            int(v["steps"]),
            int(v["breaches"]),
            _format_pct(v["compliance_pct"]),
            f"{float(v['monthly_risk_eur_est']):,.0f}".replace(",", "."),
        ])
    ttable = Table(table_data, hAlign="LEFT")
    ttable.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))
    elements.append(ttable)

elements.append(Spacer(1, 12))

elements.append(Paragraph("<b>⚠️ Actie vereist: structureel SLA-risico</b>", styles["Heading2"]))
elements.append(Spacer(1, 6))
if not upgrade_signals:
    elements.append(Paragraph(
        "Op basis van de geanalyseerde supportdata zijn geen urgente SLA-signalen vastgesteld.",
        styles["Normal"]
    ))
else:
    elements.append(Paragraph(
        "Op basis van de geanalyseerde supportdata zijn één of meerdere structurele SLA-risico’s vastgesteld.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 6))
    for s in upgrade_signals[:5]:
        elements.append(Paragraph(f"• {s['message']}", styles["Normal"]))

elements.append(Spacer(1, 12))

elements.append(Paragraph("<b>AI-gestuurde verbeteraanbevelingen</b>", styles["Heading2"]))
elements.append(Spacer(1, 6))
if not ai_advice:
    elements.append(Paragraph(
        "Op basis van de huidige dataset zijn geen prioritaire aanbevelingen berekend (onvoldoende structureel risico).",
        styles["Normal"]
    ))
else:
    elements.append(Paragraph(
        "Op basis van de geconstateerde knelpunten zijn de onderstaande verbeteracties geïdentificeerd als meest impactvol.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 8))
    for a in ai_advice:
        elements.append(Paragraph(
            f"<b>{a['title']}</b> — geschatte besparing: <b>{_format_eur(a['monthly_risk_reduction_est'])} per maand</b>",
            styles["Normal"]
        ))
        elements.append(Paragraph(a["summary"], styles["Normal"]))
        for act in a["actions"]:
            elements.append(Paragraph(f"• {act}", styles["Normal"]))
        elements.append(Spacer(1, 6))

elements.append(Spacer(1, 14))

# Aanbevolen acties (bestaand)
elements.append(Paragraph("<b>Aanbevolen acties (eerste 30 dagen)</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
if advice_items:
    for step, text in advice_items:
        elements.append(Paragraph(f"<b>{step}</b>: {text}", styles["Normal"]))
        elements.append(Spacer(1, 6))
else:
    elements.append(Paragraph("Geen significante structurele vertragingen gedetecteerd.", styles["Normal"]))

elements.append(Spacer(1, 12))

# Top knelpunten (bestaand)
elements.append(Paragraph("<b>Top knelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if summary.empty:
    elements.append(Paragraph("Geen significante procesvertragingen gedetecteerd.", styles["Normal"]))
else:
    if eur_per_hour > 0:
        table_data = [["Processtap", "Aantal", "Impact (uren)", "Impact (€)"]]
        for _, row in summary.iterrows():
            table_data.append([
                str(row["event"]),
                int(row["occurrences"]),
                f"{float(row['total_impact_hours']):.2f}",
                f"{float(row['total_impact_eur']):,.0f}".replace(",", "."),
            ])
    else:
        table_data = [["Processtap", "Aantal", "Impact (uren)"]]
        for _, row in summary.iterrows():
            table_data.append([
                str(row["event"]),
                int(row["occurrences"]),
                f"{float(row['total_impact_hours']):.2f}",
            ])

    table = Table(table_data, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))
    elements.append(table)

# Visualisaties pagina (bestaand)
elements.append(PageBreak())
elements.append(Paragraph("<b>Visualisaties</b>", styles["Title"]))
elements.append(Spacer(1, 14))

top_n = 10
chart_series = []
if not summary.empty:
    for _, row in summary.head(top_n).iterrows():
        chart_series.append((str(row["event"]), float(row["total_impact_hours"])))

chart = make_bar_chart(chart_series, f"Impact (uren) per processtap — Top {min(top_n, len(chart_series))}")
elements.append(DrawingFlowable(chart))
elements.append(Spacer(1, 10))
elements.append(Paragraph("Hoe langer de balk, hoe groter de structurele vertraging in deze stap.", styles["Normal"]))

# ✅ NEW: SLA trends per type (grafieken)
elements.append(PageBreak())
elements.append(Paragraph("<b>SLA-trends over tijd</b>", styles["Title"]))
elements.append(Spacer(1, 12))

hist_last = history[-6:] if isinstance(history, list) else []
for t in ["first_response", "waiting", "resolution"]:
    pts = []
    for i, h in enumerate(hist_last):
        v = (h.get("sla_by_type") or {}).get(t)
        if not v:
            continue
        pts.append((f"T{i+1}", float(v.get("compliance_pct", 0.0) or 0.0)))

    elements.append(Paragraph(f"<b>{t.replace('_',' ').title()}</b>", styles["Heading2"]))
    elements.append(Spacer(1, 6))
    elements.append(DrawingFlowable(make_line_chart(
        pts,
        f"Compliance trend — {t.replace('_',' ')}",
        suffix="%",
        fmt="{:.1f}",
    )))
    elements.append(Spacer(1, 14))

doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)

print(f"PDF gegenereerd: {OUTPUT_PDF}")
print(f"Metrics saved: {LAST_METRICS_PATH} (previous: {PREV_METRICS_PATH}) | History: {HISTORY_PATH}")
