import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

import matplotlib.pyplot as plt


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
CHART_PATH = UPLOAD_DIR / "impact_chart.png"

if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")


# ===============================
# CSV INLEZEN + NORMALISATIE
# ===============================
df = pd.read_csv(CSV_PATH)

COLUMN_ALIASES = {
    "case_id": ["case_id", "case", "caseid", "ticket_id"],
    "timestamp": ["timestamp", "time", "datetime", "date"],
    "event": ["event", "activity", "step", "status"],
}

normalized = {}
for canonical, options in COLUMN_ALIASES.items():
    for col in options:
        if col in df.columns:
            normalized[canonical] = col
            break

missing = set(COLUMN_ALIASES.keys()) - set(normalized.keys())
if missing:
    raise ValueError(f"Ontbrekende kolommen: {missing}")

df = df.rename(columns={v: k for k, v in normalized.items()})


# ===============================
# CLEANUP
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# DUUR + DELAYS
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (
    df["next_timestamp"] - df["timestamp"]
).dt.total_seconds() / 3600

df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]

baseline = df.groupby("event")["duration_hours"].median()
df["baseline"] = df["event"].map(baseline)

df["impact_hours"] = df["duration_hours"] - df["baseline"]
delays = df[df["impact_hours"] > 0].copy()

delays["impact_eur"] = delays["impact_hours"] * eur_per_hour

summary = (
    delays.groupby("event")
    .agg(
        occurrences=("impact_hours", "count"),
        total_impact_hours=("impact_hours", "sum"),
        total_impact_eur=("impact_eur", "sum"),
    )
    .sort_values("total_impact_hours", ascending=False)
    .reset_index()
)

total_hours = summary["total_impact_hours"].sum()
total_eur = summary["total_impact_eur"].sum()


# ===============================
# ROI SCENARIO'S
# ===============================
roi_rows = []
for pct in (0.1, 0.2, 0.3):
    roi_rows.append({
        "label": f"{int(pct*100)}% verbetering",
        "hours": total_hours * pct,
        "eur": total_eur * pct,
    })


# ===============================
# AANBEVOLEN ACTIES
# ===============================
ACTIONS = {
    "Assigned to agent": "Automatiseer toewijzing en stel SLA op eerste reactie in.",
    "Waiting for customer": "Gebruik klant-reminders en pauzeer SLA bij wachten op klant.",
    "Agent response": "Balanceer workload en introduceer WIP-limieten.",
}

recommendations = []
for _, row in summary.iterrows():
    if row["event"] in ACTIONS:
        recommendations.append(
            f"<b>{row['event']}:</b> {ACTIONS[row['event']]}"
        )


# ===============================
# VISUALISATIE
# ===============================
if not summary.empty:
    plt.figure(figsize=(6, 3))
    plt.barh(summary["event"], summary["total_impact_hours"])
    plt.xlabel("Impact (uur)")
    plt.tight_layout()
    plt.savefig(CHART_PATH)
    plt.close()


# ===============================
# PDF
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUTPUT_PDF), pagesize=A4)
elements = []

elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    f"<b>Totale impact:</b> {total_hours:.2f} uur (€{total_eur:,.0f})",
    styles["Normal"]
))
elements.append(Spacer(1, 12))


elements.append(Paragraph("<b>Aanbevolen acties (30 dagen)</b>", styles["Heading2"]))
for rec in recommendations:
    elements.append(Paragraph(rec, styles["Normal"]))
elements.append(Spacer(1, 12))


elements.append(Paragraph("<b>ROI-scenario’s</b>", styles["Heading2"]))
roi_table = [["Scenario", "Uur / maand", "€ / maand"]]
for r in roi_rows:
    roi_table.append([
        r["label"],
        f"{r['hours']:.1f}",
        f"€{r['eur']:,.0f}",
    ])

elements.append(Table(roi_table, hAlign="LEFT"))
elements.append(Spacer(1, 12))


elements.append(Paragraph("<b>Top knelpunten</b>", styles["Heading2"]))
table_data = [["Stap", "Aantal", "Impact (uur)"]]
for _, r in summary.iterrows():
    table_data.append([
        r["event"],
        int(r["occurrences"]),
        f"{r['total_impact_hours']:.2f}",
    ])

table = Table(table_data)
table.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
]))
elements.append(table)

if CHART_PATH.exists():
    elements.append(Spacer(1, 16))
    elements.append(Paragraph("<b>Visualisaties</b>", styles["Heading2"]))
    elements.append(Image(str(CHART_PATH), width=400, height=200))


doc.build(elements)


# ===============================
# METRICS
# ===============================
metrics = {
    "created_at": datetime.now(timezone.utc).isoformat(),
    "total_hours": float(total_hours),
    "total_eur": float(total_eur),
    "pdf": OUTPUT_PDF.name,
}
METRICS_PATH.write_text(json.dumps(metrics, indent=2))

print("PDF gegenereerd:", OUTPUT_PDF)
