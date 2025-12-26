# ====== analyze.py (VOLLEDIG) ======

import sys
import json
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.platypus.flowables import Flowable

# ================= SETUP =================
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
ASSETS_DIR = BASE_DIR / "assets"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / (sys.argv[2] if len(sys.argv) > 2 else "process_report.pdf")
EUR_PER_HOUR = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0

LAST_METRICS = UPLOAD_DIR / "last_metrics.json"
PREV_METRICS = UPLOAD_DIR / "previous_metrics.json"
HISTORY = DATA_DIR / "metrics_history.json"

# ================= HELPERS =================
def read_json(p): return json.loads(p.read_text()) if p.exists() else None
def write_json(p, d): p.write_text(json.dumps(d, indent=2, ensure_ascii=False))

def roll_metrics():
    if LAST_METRICS.exists():
        shutil.copyfile(LAST_METRICS, PREV_METRICS)

# ================= LOAD CSV =================
df = pd.read_csv(CSV_PATH)
df.columns = [c.lower() for c in df.columns]

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(["case_id", "timestamp"])

df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_h"] = (df["next_ts"] - df["timestamp"]).dt.total_seconds() / 3600
df = df.dropna(subset=["duration_h"])

# ================= BASELINE & IMPACT =================
baseline = df.groupby("event")["duration_h"].median()
df["baseline"] = df["event"].map(baseline)

df["delay"] = df["duration_h"] > df["baseline"] * 1.5
df["impact_h"] = (df["duration_h"] - df["baseline"]).clip(lower=0)
df["impact_eur"] = df["impact_h"] * EUR_PER_HOUR

impact_h = df["impact_h"].sum()
impact_eur = df["impact_eur"].sum()

# ================= PERIOD =================
period_hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
monthly_factor = (30 * 24) / period_hours if period_hours > 1 else 0

monthly_eur = impact_eur * monthly_factor
fte = (impact_h * monthly_factor) / 160 if monthly_factor else 0

# ================= SLA TYPES =================
def sla_type(ev):
    e = ev.lower()
    if "waiting" in e: return "waiting"
    if "resolved" in e or "closed" in e: return "resolution"
    return "first_response"

df["sla_type"] = df["event"].apply(sla_type)
df["sla_breach"] = df["duration_h"] > df["baseline"] * 1.2

sla_by_type = {}
for t in ["first_response", "waiting", "resolution"]:
    sub = df[df["sla_type"] == t]
    if sub.empty: continue
    breaches = sub["sla_breach"].sum()
    compliance = 100 * (1 - breaches / len(sub))
    risk = sub[sub["sla_breach"]]["impact_h"].sum() * EUR_PER_HOUR * monthly_factor

    sla_by_type[t] = {
        "compliance_pct": round(compliance, 1),
        "monthly_risk_eur_est": round(risk, 0)
    }

# ================= HISTORY =================
history = read_json(HISTORY) or []
history.append({
    "date": datetime.now(timezone.utc).isoformat(),
    "sla_by_type": sla_by_type
})
write_json(HISTORY, history)

# ================= UPGRADE SIGNALEN (NL) =================
upgrade_signals = []
for t, v in sla_by_type.items():
    if v["compliance_pct"] < 90:
        upgrade_signals.append({
            "severity": "hoog",
            "message": f"De SLA-compliance voor {t.replace('_',' ')} ligt op {v['compliance_pct']}%."
        })
    if v["monthly_risk_eur_est"] > 1000:
        upgrade_signals.append({
            "severity": "middel",
            "message": f"Het geschatte financiële risico voor {t.replace('_',' ')} bedraagt €{v['monthly_risk_eur_est']:,} per maand."
        })

# ================= AI ADVIES (NL) =================
ai_advice = []
if "waiting" in sla_by_type:
    ai_advice.append({
        "title": "Beperk wachttijd bij klanten",
        "summary": "Langdurige wachttijden veroorzaken structureel capaciteitsverlies.",
        "monthly_risk_reduction_est": int(sla_by_type["waiting"]["monthly_risk_eur_est"] * 0.4)
    })

# ================= SAVE METRICS =================
roll_metrics()
metrics = {
    "impact": {
        "monthly_eur": round(monthly_eur, 0),
        "fte_equivalent": round(fte, 2)
    },
    "sla_by_type": sla_by_type,
    "upgrade_signals": upgrade_signals,
    "ai_advice": ai_advice,
    "pdf": OUTPUT_PDF.name
}
write_json(LAST_METRICS, metrics)

# ================= PDF =================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUTPUT_PDF), pagesize=A4)
elements = []

elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    f"Geschatte maandimpact: <b>€{monthly_eur:,.0f}</b><br/>"
    f"FTE-equivalent: <b>{fte:.2f}</b>",
    styles["Normal"]
))

elements.append(PageBreak())
elements.append(Paragraph("<b>SLA Intelligence</b>", styles["Heading2"]))

for t, v in sla_by_type.items():
    elements.append(Paragraph(
        f"{t.replace('_',' ').title()}: "
        f"Compliance {v['compliance_pct']}% · "
        f"Risico €{v['monthly_risk_eur_est']:,.0f}/maand",
        styles["Normal"]
    ))

doc.build(elements)

print("Analyse + PDF gegenereerd")

