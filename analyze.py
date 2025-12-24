# analyze.py — Prolixia
# SLA Intelligence + AI Advice + Trends + Trendgrafieken + Trend per SLA-type

import sys, json, shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    PageBreak
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

UPLOAD.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)

CSV = UPLOAD / "events.csv"
OUT = UPLOAD / output_pdf

LAST = UPLOAD / "last_metrics.json"
HISTORY = DATA / "metrics_history.json"

# ===============================
# HELPERS
# ===============================
def rj(p):
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

def wj(p, d):
    p.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")

def fe(x): return f"€{x:,.0f}".replace(",", ".")
def fp(x): return f"{x:.1f}%".replace(".", ",")

# ===============================
# SLA TYPE MAPPING
# ===============================
def sla_type(event: str) -> str:
    e = event.lower()
    if "assign" in e or "created" in e or "response" in e:
        return "first_response"
    if "resolved" in e or "closed" in e:
        return "resolution"
    if "waiting" in e:
        return "waiting"
    return "other"

# ===============================
# LOAD CSV
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

df["sla_type"] = df["event"].apply(sla_type)

period_hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
can_extrapolate = period_hours >= 1

# ===============================
# BASELINE + SLA
# ===============================
baseline = df.groupby("event")["dur_h"].median()
df["baseline"] = df["event"].map(baseline)
df["sla_breach"] = df["dur_h"] > (df["baseline"] * 1.2)
df["impact_eur"] = (df["dur_h"] - df["baseline"]).clip(lower=0) * eur_per_hour

# ===============================
# SLA METRICS PER TYPE
# ===============================
sla_by_type = {}

for t in ["first_response", "resolution", "waiting"]:
    sub = df[df["sla_type"] == t]
    if sub.empty:
        continue

    steps = len(sub)
    breaches = int(sub["sla_breach"].sum())
    compliance = 100 * (steps - breaches) / steps if steps else 0

    risk = sub.loc[sub["sla_breach"], "impact_eur"].sum()
    monthly_risk = risk * (720 / period_hours) if can_extrapolate else risk

    sla_by_type[t] = {
        "compliance_pct": round(compliance, 1),
        "monthly_risk_eur_est": round(monthly_risk, 0),
    }

# ===============================
# SAVE METRICS + HISTORY
# ===============================
metrics = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "sla_by_type": sla_by_type
}

wj(LAST, metrics)

history = rj(HISTORY)
history.append(metrics)
wj(HISTORY, history)

# ===============================
# TREND DATA PER TYPE
# ===============================
trend_by_type = {}

if len(history) >= 2:
    prev = history[-2]["sla_by_type"]
    curr = history[-1]["sla_by_type"]

    for t in curr:
        if t not in prev:
            continue
        trend_by_type[t] = {
            "compliance_delta_pp": round(
                curr[t]["compliance_pct"] - prev[t]["compliance_pct"], 1
            ),
            "risk_delta_eur": round(
                curr[t]["monthly_risk_eur_est"] - prev[t]["monthly_risk_eur_est"], 0
            ),
        }

metrics["sla_trend_by_type"] = trend_by_type
wj(LAST, metrics)

# ===============================
# TREND GRAPH HELPER
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

def make_line_chart(points, title, suffix="", fmt="{:.1f}"):
    w, h = 520, 260
    d = Drawing(w, h)
    d.add(String(0, h - 16, title, fontName="Helvetica-Bold", fontSize=13))

    if len(points) < 2:
        d.add(String(0, h - 40, "Nog onvoldoende data.", fontSize=10))
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
        d.add(String(x - 10, y + 5, fmt.format(val) + suffix, fontSize=8))
        if prev:
            d.add(Rect(prev[0], prev[1], x - prev[0], 1.5,
                       fillColor=colors.HexColor("#2563eb"), strokeColor=None))
        prev = (x, y)
    return d

# ===============================
# PDF
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUT), pagesize=A4,
                        rightMargin=36, leftMargin=36,
                        topMargin=72, bottomMargin=36)
els = []

els.append(Paragraph("<b>Prolixia – SLA Trends per type</b>", styles["Title"]))
els.append(Spacer(1, 12))

for t, vals in sla_by_type.items():
    els.append(Paragraph(f"<b>{t.replace('_',' ').title()}</b>", styles["Heading2"]))
    els.append(Paragraph(
        f"Compliance: <b>{fp(vals['compliance_pct'])}</b> • "
        f"Risico: <b>{fe(vals['monthly_risk_eur_est'])}/maand</b>",
        styles["Normal"]
    ))

    # trend chart
    pts = []
    for i, h in enumerate(history[-6:]):
        if t in h["sla_by_type"]:
            pts.append((f"T{i+1}", h["sla_by_type"][t]["compliance_pct"]))

    els.append(Spacer(1, 8))
    els.append(DrawingFlowable(
        make_line_chart(
            pts,
            f"Compliance trend — {t.replace('_',' ')}",
            "%",
            "{:.1f}"
        )
    ))
    els.append(Spacer(1, 14))

doc.build(els)

print("PDF generated:", OUT)
