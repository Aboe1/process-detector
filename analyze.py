import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.platypus.flowables import Flowable


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
# SLA / KPI BEREKENINGEN (NIEUW)
# ===============================
total_tickets = int(df["case_id"].nunique())

ticket_durations = df.groupby("case_id")["duration_hours"].sum()
avg_wait_per_ticket = float(ticket_durations.mean()) if not ticket_durations.empty else 0.0

SLA_HOURS = 24
sla_breach_pct = float((ticket_durations > SLA_HOURS).mean() * 100) if not ticket_durations.empty else 0.0

avg_step_wait = df.groupby("event")["duration_hours"].mean().sort_values(ascending=False)
top_sla_steps = avg_step_wait.head(5)  # top 5 voor grafiek

monthly_loss_estimate = (avg_wait_per_ticket * eur_per_hour * total_tickets) if eur_per_hour > 0 else 0.0
potential_saving_estimate = monthly_loss_estimate * 0.2


# ===============================
# GRAFIEK (BALKEN) FLOWABLE
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

def make_bar_chart(data: list[tuple[str, float]], title: str, width: int = 440, height: int = 220) -> Drawing:
    """
    data: list of (label, value) - values are hours.
    """
    d = Drawing(width, height)

    # Title
    d.add(String(0, height - 14, title, fontName="Helvetica-Bold", fontSize=12, fillColor=colors.HexColor("#0f172a")))

    if not data:
        d.add(String(0, height - 40, "Geen data beschikbaar.", fontName="Helvetica", fontSize=10, fillColor=colors.black))
        return d

    max_val = max(v for _, v in data) if data else 1
    if max_val <= 0:
        max_val = 1

    left_label_w = 170
    chart_w = width - left_label_w - 60
    top = height - 40
    row_h = (top - 10) / len(data)

    # Background axis line
    d.add(Rect(left_label_w, 8, chart_w, 1, fillColor=colors.lightgrey, strokeColor=colors.lightgrey))

    for i, (label, val) in enumerate(data):
        y = top - (i + 1) * row_h + 8
        bar_h = row_h * 0.55
        bar_w = (val / max_val) * chart_w

        # Label
        d.add(String(0, y + 2, str(label)[:28], fontName="Helvetica", fontSize=9, fillColor=colors.black))

        # Bar
        d.add(Rect(left_label_w, y, bar_w, bar_h, fillColor=colors.HexColor("#0f172a"), strokeColor=None))

        # Value
        d.add(String(left_label_w + chart_w + 6, y + 2, f"{val:.1f}u", fontName="Helvetica", fontSize=9, fillColor=colors.black))

    return d


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
elements.append(Paragraph("<b>Prolixia – Support SLA Analyse Rapport</b>", styles["Title"]))
elements.append(Spacer(1, 10))

elements.append(Paragraph(
    "Dit rapport toont structurele procesvertragingen en SLA-impact op basis van event-log analyse.",
    styles["Normal"],
))
elements.append(Spacer(1, 12))

if eur_per_hour > 0:
    elements.append(Paragraph(f"<b>Kostprijs per uur:</b> €{eur_per_hour:.2f}", styles["Normal"]))
    elements.append(Spacer(1, 6))

# ===============================
# SLA / KPI OVERZICHT
# ===============================
elements.append(Paragraph("<b>SLA & KPI Overzicht</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

elements.append(Paragraph(f"<b>Aantal tickets:</b> {total_tickets}", styles["Normal"]))
elements.append(Spacer(1, 4))

elements.append(Paragraph(f"<b>Gemiddelde wachttijd per ticket:</b> {avg_wait_per_ticket:.2f} uur", styles["Normal"]))
elements.append(Spacer(1, 4))

elements.append(Paragraph(f"<b>Tickets boven SLA (24 uur):</b> {sla_breach_pct:.1f}%", styles["Normal"]))
elements.append(Spacer(1, 8))

elements.append(Paragraph(
    f"<b>Totale impact (delays vs baseline):</b> {total_impact_hours:.2f} uur"
    + (f" (≈ €{total_impact_eur:,.2f})" if eur_per_hour > 0 else ""),
    styles["Normal"],
))
elements.append(Spacer(1, 8))

if eur_per_hour > 0:
    elements.append(Paragraph(f"<b>Geschatte maandelijkse kosten (wachttijd):</b> €{monthly_loss_estimate:,.0f}", styles["Normal"]))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(f"<b>Potentiële besparing (20%):</b> €{potential_saving_estimate:,.0f}", styles["Normal"]))
    elements.append(Spacer(1, 12))

# ===============================
# GRAFIEK: TOP SLA-VEROORZAKERS
# ===============================
elements.append(Paragraph("<b>Top SLA-veroorzakers (gemiddelde wachttijd per stap)</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

chart_data = [(str(k), float(v)) for k, v in top_sla_steps.items()]
chart = make_bar_chart(chart_data, "Gemiddelde wachttijd per stap (uren)")
elements.append(DrawingFlowable(chart))
elements.append(Spacer(1, 14))

elements.append(Paragraph(
    "Hoe langer de balk, hoe meer structurele wachttijd in deze stap.",
    styles["Normal"],
))
elements.append(Spacer(1, 18))

# ===============================
# TOP PROCESKNELPUNTEN (jouw bestaande tabel)
# ===============================
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
    "total_tickets": total_tickets,
    "avg_wait_per_ticket": avg_wait_per_ticket,
    "sla_breach_pct": sla_breach_pct,
    "total_impact_hours": total_impact_hours,
    "total_impact_eur": total_impact_eur,
    "monthly_loss_estimate": monthly_loss_estimate,
    "potential_saving_estimate": potential_saving_estimate,
    "pdf": OUTPUT_PDF.name,
}
METRICS_PATH.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"PDF gegenereerd: {OUTPUT_PDF}")
print(f"Metrics: {METRICS_PATH}")
