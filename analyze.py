import sys
import json
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


# ===============================
# ARGS
# ===============================
def _parse_float(x: str, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


eur_per_hour = _parse_float(sys.argv[1], 0.0) if len(sys.argv) > 1 else 0.0
output_pdf_name = sys.argv[2] if len(sys.argv) > 2 else "process_report.pdf"

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / output_pdf_name
METRICS_PATH = UPLOAD_DIR / "last_metrics.json"

if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")


# ===============================
# CSV INLEZEN
# ===============================
df = pd.read_csv(CSV_PATH)

COLUMN_ALIASES = {
    "case_id": ["case_id", "case", "caseid", "ticket_id"],
    "timestamp": ["timestamp", "time", "datetime"],
    "event": ["event", "activity", "step", "status"],
}

normalized = {}
for canon, options in COLUMN_ALIASES.items():
    for col in options:
        if col in df.columns:
            normalized[canon] = col
            break

df = df.rename(columns={v: k for k, v in normalized.items()})

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# DUUR + IMPACT
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (
    df["next_timestamp"] - df["timestamp"]
).dt.total_seconds().div(3600)

df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]

baseline = df.groupby("event")["duration_hours"].median()
df["baseline"] = df["event"].map(baseline)

df["impact_hours"] = (df["duration_hours"] - df["baseline"]).clip(lower=0)

summary = (
    df[df["impact_hours"] > 0]
    .groupby("event")
    .agg(
        count=("impact_hours", "count"),
        impact=("impact_hours", "sum"),
    )
    .sort_values("impact", ascending=False)
    .reset_index()
)

total_impact_hours = summary["impact"].sum()
total_impact_eur = total_impact_hours * eur_per_hour


# ===============================
# ADVIESLOGICA
# ===============================
ADVICE_MAP = {
    "assigned": "Automatiseer toewijzing en stel SLA op eerste reactie in.",
    "waiting": "Gebruik klant-reminders en pauzeer SLA bij wachten op klant.",
    "response": "Balanceer workload en introduceer WIP-limieten.",
    "triage": "Versnel triage met vaste categorieën.",
    "created": "Automatiseer ticketcreatie via formulieren of integraties.",
}

def advice_for(step):
    s = step.lower()
    for k, v in ADVICE_MAP.items():
        if k in s:
            return v
    return "Analyseer deze stap op standaardisatie en automatisering."


# ===============================
# PDF START
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(
    str(OUTPUT_PDF),
    pagesize=A4,
    rightMargin=36,
    leftMargin=36,
    topMargin=36,
    bottomMargin=36,
)

elements = []

elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    f"<b>Totale impact:</b> {total_impact_hours:.2f} uur (€{total_impact_eur:,.0f})",
    styles["Normal"],
))
elements.append(Spacer(1, 14))


# ===============================
# AANBEVOLEN ACTIES
# ===============================
elements.append(Paragraph("<b>Aanbevolen acties (30 dagen)</b>", styles["Heading2"]))
elements.append(Spacer(1, 6))

for _, row in summary.head(3).iterrows():
    elements.append(Paragraph(
        f"<b>{row['event']}</b>: {advice_for(row['event'])}",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 4))


# ===============================
# TABEL
# ===============================
elements.append(Spacer(1, 14))
elements.append(Paragraph("<b>Top knelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 6))

table_data = [["Stap", "Aantal", "Impact (uur)"]]
for _, r in summary.iterrows():
    table_data.append([r["event"], int(r["count"]), f"{r['impact']:.2f}"])

table = Table(table_data)
table.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
]))
elements.append(table)


# ===============================
# PAGINA 2 – VISUALISATIES
# ===============================
elements.append(PageBreak())
elements.append(Paragraph("<b>Visualisaties</b>", styles["Title"]))
elements.append(Spacer(1, 20))

max_impact = summary["impact"].max() if not summary.empty else 1
bar_width = 400
bar_height = 14
y = 0

drawing = Drawing(500, 300)

for _, row in summary.iterrows():
    width = (row["impact"] / max_impact) * bar_width
    drawing.add(Rect(0, y, width, bar_height, fillColor=colors.HexColor("#4F81BD")))
    drawing.add(String(width + 5, y + 2, f"{row['impact']:.1f} u", fontSize=9))
    drawing.add(String(0, y + 16, row["event"], fontSize=9))
    y += 30

elements.append(drawing)

doc.build(elements)


# ===============================
# METRICS
# ===============================
METRICS_PATH.write_text(json.dumps({
    "created": datetime.now(timezone.utc).isoformat(),
    "total_hours": total_impact_hours,
    "total_eur": total_impact_eur,
    "pdf": OUTPUT_PDF.name,
}, indent=2))

print("PDF gegenereerd:", OUTPUT_PDF)
