import pandas as pd
from pathlib import Path
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

# ===============================
# PADEN
# ===============================
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / "process_report.pdf"

if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")

df = pd.read_csv(CSV_PATH)

# ===============================
# KOLOM NORMALISATIE (SaaS-proof)
# ===============================
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
# BASIC CLEANUP
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
df = df[df["duration_hours"] >= 0]  # safety

# ===============================
# NORMAALGEDRAG & AFWIJKINGEN
# ===============================
baseline = df.groupby("event")["duration_hours"].median().rename("baseline_hours")
df = df.join(baseline, on="event")

df["is_delay"] = df["duration_hours"] > 1.5 * df["baseline_hours"]
df["impact_hours"] = df["duration_hours"] - df["baseline_hours"]

delays = df[df["is_delay"]].copy()

summary = (
    delays.groupby("event")
    .agg(
        occurrences=("impact_hours", "count"),
        total_impact_hours=("impact_hours", "sum"),
        avg_impact_hours=("impact_hours", "mean"),
    )
    .sort_values("total_impact_hours", ascending=False)
    .reset_index()
)

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
elements.append(Spacer(1, 16))

elements.append(Paragraph("<b>Top procesknelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if summary.empty:
    elements.append(Paragraph("Geen significante procesvertragingen gedetecteerd.", styles["Normal"]))
else:
    table_data = [["Processtap", "Aantal keer", "Totale impact (uren)", "Gem. impact (uren)"]]
    for _, row in summary.iterrows():
        table_data.append([
            str(row["event"]),
            int(row["occurrences"]),
            f"{row['total_impact_hours']:.2f}",
            f"{row['avg_impact_hours']:.2f}",
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
print(f"PDF gegenereerd: {OUTPUT_PDF}")
