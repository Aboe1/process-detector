# analyze.py — Prolixia (SLA Intelligence + AI + Trends + Trendgrafieken)

import sys, json, shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.platypus.flowables import Flowable

# ===============================
# ARGS
# ===============================
def _f(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

eur_per_hour = _f(sys.argv[1]) if len(sys.argv) > 1 else 0.0
output_pdf = sys.argv[2] if len(sys.argv) > 2 else "process_report.pdf"

BASE = Path(__file__).resolve().parent
UPLOAD = BASE / "uploads"
DATA = BASE / "data"
ASSETS = BASE / "assets"

UPLOAD.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)

CSV = UPLOAD / "events.csv"
OUT = UPLOAD / output_pdf

LAST = UPLOAD / "last_metrics.json"
PREV = UPLOAD / "previous_metrics.json"
HISTORY = DATA / "metrics_history.json"
LOGO = ASSETS / "logo.png"

# ===============================
# HELPERS
# ===============================
def rj(p):
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def wj(p, d):
    p.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")

def roll():
    if LAST.exists():
        try:
            shutil.copyfile(LAST, PREV)
        except Exception:
            pass

def fe(x): return f"€{x:,.0f}".replace(",", ".")
def fp(x): return f"{x:.1f}%".replace(".", ",")

# ===============================
# AI RULES
# ===============================
AI_RULES = [
    {
        "title": "Versnel eerste reactie",
        "keywords": ["assigned", "created", "response"],
        "actions": [
            "Stel SLA < 2 uur in",
            "Activeer automatische tickettoewijzing",
            "Monitor piekbelasting per kanaal"
        ],
        "reduction": 0.25
    },
    {
        "title": "Verkort oplostijd",
        "keywords": ["resolved", "closed"],
        "actions": [
            "Escalatie na 24 uur",
            "Splits complexe tickets",
            "Analyseer herhaalproblemen"
        ],
        "reduction": 0.30
    },
    {
        "title": "Beperk wachttijd",
        "keywords": ["waiting"],
        "actions": [
            "Pauzeer SLA bij wachten op klant",
            "Automatische reminders",
            "Sluit inactieve tickets"
        ],
        "reduction": 0.40
    }
]

# ===============================
# CSV LOAD
# ===============================
if not CSV.exists():
    raise FileNotFoundError("uploads/events.csv ontbreekt")

df = pd.read_csv(CSV)
df.columns = [c.lower() for c in df.columns]
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["case_id", "event", "timestamp"])
df = df.sort_values(["case_id", "timestamp"])

df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
df["dur_h"] = (df["next_ts"] - df["timestamp"]).dt.total_seconds() / 3600
df = df.dropna(subset=["dur_h"])
df = df[df["dur_h"] >= 0]

period_hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
can_extrapolate = period_hours >= 1

# ===============================
# BASELINE + IMPACT
# ===============================
baseline = df.groupby("event")["dur_h"].median()
df["baseline"] = df["event"].map(baseline)
df["impact_h"] = (df["dur_h"] - df["baseline"]).clip(lower=0)
df["impact_eur"] = df["impact_h"] * eur_per_hour

total_impact_eur = df["impact_eur"].sum()

# ===============================
# SLA
# ===============================
df["sla_breach"] = df["dur_h"] > (df["baseline"] * 1.2)
steps = len(df)
breaches = int(df["sla_breach"].sum())
compliance = 100 * (steps - breaches) / steps if steps else 0

sla_risk = df.loc[df["sla_breach"], "impact_eur"].sum()
monthly_risk = sla_risk * (720 / period_hours) if can_extrapolate else sla_risk

# ===============================
# AI ADVICE
# ===============================
ai_advice = []
for rule in AI_RULES:
    if any(df["event"].str.contains(k, case=False).any() for k in rule["keywords"]):
        reduction = monthly_risk * rule["reduction"]
        if reduction > 0:
            ai_advice.append({
                "title": rule["title"],
                "actions": rule["actions"],
                "monthly_risk_reduction_est": round(reduction, 0)
            })
ai_advice = ai_advice[:3]

# ===============================
# METRICS SAVE
# ===============================
roll()

metrics = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "sla": {
        "compliance_pct": round(compliance, 1),
        "monthly_risk_eur_est": round(monthly_risk, 0)
    },
    "ai_advice": ai_advice
}

wj(LAST, metrics)

# ===============================
# HISTORY (TRENDS)
# ===============================
history = rj(HISTORY) or []
history.append({
    "generated_at": metrics["generated_at"],
    "sla": metrics["sla"]
})
wj(HISTORY, history)

trend = None
if len(history) >= 2:
    prev, cur = history[-2], history[-1]
    trend = {
        "compliance_delta_pp": round(
            cur["sla"]["compliance_pct"] - prev["sla"]["compliance_pct"], 1
        ),
        "risk_delta_eur": round(
            cur["sla"]["monthly_risk_eur_est"] - prev["sla"]["monthly_risk_eur_est"], 0
        )
    }
metrics["sla_trend"] = trend
wj(LAST, metrics)

# ===============================
# TREND GRAPH (LINE)
# ===============================
class DrawingFlowable(Flowable):
    def __init__(self, drawing):
        super().__init__()
        self.drawing = drawing
        self.width = drawing.width
        self.height = drawing.height

    def draw(self):
        from reportlab.graphics import renderPDF
        renderPDF.draw(self.drawing, self.canv, 0, 0)

def make_line_chart(points, title, value_fmt="{:.1f}", suffix=""):
    w, h = 520, 300
    d = Drawing(w, h)
    d.add(String(0, h - 16, title, fontName="Helvetica-Bold", fontSize=13))

    if len(points) < 2:
        d.add(String(0, h - 40, "Nog onvoldoende data voor trendgrafiek.", fontSize=10))
        return d

    vals = [v for _, v in points]
    min_v, max_v = min(vals), max(vals)
    if min_v == max_v:
        max_v += 1

    left, bottom = 60, 40
    right, top = w - 20, h - 50
    step = (right - left) / (len(points) - 1)

    prev = None
    for i, (label, val) in enumerate(points):
        x = left + i * step
        y = bottom + (val - min_v) / (max_v - min_v) * (top - bottom)
        d.add(String(x - 10, bottom - 15, label, fontSize=8))
        d.add(String(x - 10, y + 5, value_fmt.format(val) + suffix, fontSize=8))
        if prev:
            d.add(Rect(prev[0], prev[1], x - prev[0], 1.5,
                       fillColor=colors.HexColor("#2563eb"), strokeColor=None))
        prev = (x, y)
    return d

# ===============================
# PDF
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUT), pagesize=A4, rightMargin=36, leftMargin=36, topMargin=72, bottomMargin=36)
els = []

els.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
els.append(Spacer(1, 8))
els.append(Paragraph(f"SLA-compliance: <b>{fp(compliance)}</b>", styles["Normal"]))
els.append(Paragraph(f"Maandelijks SLA-risico: <b>{fe(monthly_risk)}</b>", styles["Normal"]))
els.append(Spacer(1, 10))

els.append(Paragraph("<b>AI-gestuurde verbeteracties</b>", styles["Heading2"]))
for a in ai_advice:
    els.append(Paragraph(f"<b>{a['title']}</b> – ~{fe(a['monthly_risk_reduction_est'])}/maand", styles["Normal"]))
    for act in a["actions"]:
        els.append(Paragraph(f"• {act}", styles["Normal"]))
    els.append(Spacer(1, 6))

if trend:
    els.append(Spacer(1, 12))
    els.append(Paragraph("<b>SLA-ontwikkeling</b>", styles["Heading2"]))
    els.append(Paragraph(f"Compliance verandering: <b>{trend['compliance_delta_pp']:+.1f} pp</b>", styles["Normal"]))
    els.append(Paragraph(f"Risico verandering: <b>{fe(trend['risk_delta_eur'])}/maand</b>", styles["Normal"]))

els.append(PageBreak())
els.append(Paragraph("<b>SLA-trends over tijd</b>", styles["Title"]))
els.append(Spacer(1, 12))

pts_comp = [(f"T{i+1}", h["sla"]["compliance_pct"]) for i, h in enumerate(history[-6:])]
pts_risk = [(f"T{i+1}", h["sla"]["monthly_risk_eur_est"]) for i, h in enumerate(history[-6:])]

els.append(DrawingFlowable(make_line_chart(pts_comp, "SLA-compliance (%)", "{:.1f}", "%")))
els.append(Spacer(1, 16))
els.append(DrawingFlowable(make_line_chart(pts_risk, "Maandelijks SLA-risico (€)", "{:.0f}", "€")))

doc.build(els)
print("PDF generated:", OUT)
