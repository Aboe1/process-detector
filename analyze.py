import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors


# ===============================
# ARGS
# ===============================
# argv[1] = eur_per_hour
# argv[2] = output_pdf_filename (optional)
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
# CSV INLEZEN + KOLOM NORMALISATIE
# ===============================
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
    raise ValueError(
        f"Ontbrekende verplichte kolommen: {missing}. "
        f"Gevonden kolommen: {list(df.columns)}"
    )

df = df.rename(columns={v: k for k, v in normalized.items()})


# ===============================
# CLEANUP + SORT
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# DUUR PER STAP
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (df["next_timestamp"] - df["timestamp"]).dt.total_seconds().div(3600)
df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]


# ===============================
# BASELINE + DELAYS
# ===============================
baseline = df.groupby("event")["duration_hours"].median().rename("baseline_hours")
df = df.join(baseline, on="event")

df["is_delay"] = df["duration_hours"] > 1.5 * df["baseline_hours"]
df["impact_hours"] = df["duration_hours"] - df["baseline_hours"]

delays = df[df["is_delay"]].copy()

# euro impact
if eur_per_hour > 0:
    delays["impact_eur"] = delays["impact_hours"] * eur_per_hour
else:
    delays["impact_eur"] = 0.0

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
# PDF GENERATIE
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
elements.append(Paragraph("<b>Process Detector – Analyse Rapport</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    "Dit rapport toont structurele procesvertragingen op basis van event-log analyse.",
    styles["Normal"],
))
elements.append(Spacer(1, 12))

if eur_per_hour > 0:
    elements.append(Paragraph(
        f"<b>Kostprijs per uur:</b> €{eur_per_hour:.2f}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 6))

elements.append(Paragraph(
    f"<b>Totale impact:</b> {total_impact_hours:.2f} uur"
    + (f" (≈ €{total_impact_eur:,.2f})" if eur_per_hour > 0 else ""),
    styles["Normal"],
))
elements.append(Spacer(1, 16))

elements.append(Paragraph("<b>Top procesknelpunten</b>", styles["Heading2"]))
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
                f"{row['total_impact_hours']:.2f}",
                f"€{row['total_impact_eur']:.2f}",
            ])
    else:
        table_data = [["Processtap", "Aantal", "Impact (uren)"]]
        for _, row in summary.iterrows():
            table_data.append([
                str(row["event"]),
                int(row["occurrences"]),
                f"{row['total_impact_hours']:.2f}",
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

doc.build(elements)

# ===============================
# METRICS OPSLAAN VOOR APP (history)
# ===============================
metrics = {
    "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "eur_per_hour": eur_per_hour,
    "total_impact_hours": total_impact_hours,
    "total_impact_eur": total_impact_eur,
    "pdf": OUTPUT_PDF.name,
}
METRICS_PATH.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"PDF gegenereerd: {OUTPUT_PDF}")
print(f"Metrics: {METRICS_PATH}")

