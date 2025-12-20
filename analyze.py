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

UPLOAD_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / output_pdf_name
METRICS_PATH = UPLOAD_DIR / "last_metrics.json"

LOGO_PATH = ASSETS_DIR / "logo.png"   # mag ontbreken!


# ===============================
# CSV INLEZEN
# ===============================
if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")

df = pd.read_csv(CSV_PATH)

COLUMN_ALIASES = {
    "case_id": ["case_id", "case", "ticket_id", "order_id"],
    "timestamp": ["timestamp", "time", "datetime"],
    "event": ["event", "activity", "step", "status"],
}

normalized = {}
for canonical, options in COLUMN_ALIASES.items():
    for col in options:
        if col in df.columns:
            normalized[canonical] = col
            break

missing = set(COLUMN_ALIASES) - set(normalized)
if missing:
    raise ValueError(f"Ontbrekende kolommen: {missing}")

df = df.rename(columns={v: k for k, v in normalized.items()})


# ===============================
# CLEAN + SORT
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# DUUR
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (
    df["next_timestamp"] - df["timestamp"]
).dt.total_seconds() / 3600

df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]


# ===============================
# SLA / DELAYS
# ===============================
baseline = df.groupby("event")["duration_hours"].median()
df["baseline"] = df["event"].map(baseline)

df["impact_hours"] = df["duration_hours"] - df["baseline"]
delays = df[df["impact_hours"] > 0].copy()

if eur_per_hour > 0:
    delays["impact_eur"] = delays["impact_hours"] * eur_per_hour
else:
    delays["impact_eur"] = 0.0

summary = (
    delays.groupby("event")
    .agg(
        count=("impact_hours", "count"),
        total_hours=("impact_hours", "sum"),
        total_eur=("impact_eur", "sum"),
    )
    .sort_values("total_hours", ascending=False)
    .reset_index()
)

total_hours = float(summary["total_hours"].sum()) if not summary.empty else 0.0
total_eur = float(summary["total_eur"].sum()) if not summary.empty else 0.0


# ===============================
# PDF
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(
    str(OUTPUT_PDF),
    pagesize=A4,
    leftMargin=36,
    rightMargin=36,
    topMargin=72,
    bottomMargin=36,
)

elements = []

elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    "Dit rapport toont structurele wachttijden en SLA-overtredingen "
    "op basis van support event-logs.",
    styles["Normal"]
))
elements.append(Spacer(1, 12))

elements.append(Paragraph(
    f"<b>Totale impact:</b> {total_hours:.2f} uur"
    + (f" (€{total_eur:,.2f})" if eur_per_hour > 0 else ""),
    styles["Normal"]
))
elements.append(Spacer(1, 16))

elements.append(Paragraph("<b>Top knelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if summary.empty:
    elements.append(Paragraph("Geen significante vertragingen gevonden.", styles["Normal"]))
else:
    table_data = [["Stap", "Aantal", "Impact (uur)"]]
    for _, r in summary.iterrows():
        table_data.append([
            r["event"],
            int(r["count"]),
            f"{r['total_hours']:.2f}"
        ])

    table = Table(table_data, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))
    elements.append(table)


def header_footer(canvas, doc):
    canvas.saveState()

    # Logo (optioneel, faalt NOOIT)
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
    canvas.drawString(
        36,
        28,
        f"Prolixia • {datetime.now().strftime('%d-%m-%Y')}"
    )
    canvas.drawRightString(
        A4[0] - 36,
        28,
        f"Pagina {doc.page}"
    )

    canvas.restoreState()


doc.build(
    elements,
    onFirstPage=header_footer,
    onLaterPages=header_footer
)


# ===============================
# METRICS
# ===============================
metrics = {
    "generated": datetime.now(timezone.utc).isoformat(),
    "total_hours": total_hours,
    "total_eur": total_eur,
    "pdf": OUTPUT_PDF.name,
}

METRICS_PATH.write_text(
    json.dumps(metrics, indent=2),
    encoding="utf-8"
)

print("PDF gegenereerd:", OUTPUT_PDF)
