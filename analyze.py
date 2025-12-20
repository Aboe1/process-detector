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
# PATHS & BRANDING
# ===============================
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
ASSETS_DIR = BASE_DIR / "assets"
UPLOAD_DIR.mkdir(exist_ok=True)

LOGO_PATH = ASSETS_DIR / "logo.png"
PRIMARY_COLOR = colors.HexColor("#0f172a")
SECONDARY_COLOR = colors.HexColor("#64748b")


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
    raise ValueError(f"Ontbrekende kolommen: {missing}")

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
df["duration_hours"] = (
    df["next_timestamp"] - df["timestamp"]
).dt.total_seconds().div(3600)

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
# SLA / KPI
# ===============================
total_tickets = int(df["case_id"].nunique())
ticket_durations = df.groupby("case_id")["duration_hours"].sum()

avg_wait_per_ticket = float(ticket_durations.mean()) if not ticket_durations.empty else 0.0
sla_breach_pct = float((ticket_durations > 24).mean() * 100) if not ticket_durations.empty else 0.0

avg_step_wait = df.groupby("event")["duration_hours"].mean().sort_values(ascending=False)
top_steps = avg_step_wait.head(5)

monthly_loss = avg_wait_per_ticket * eur_per_hour * total_tickets if eur_per_hour > 0 else 0.0
potential_saving = monthly_loss * 0.2


# ===============================
# GRAFIEK FLOWABLE
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


def make_bar_chart(data, title, width=440, height=220):
    d = Drawing(width, height)
    d.add(String(0, height - 14, title, fontName="Helvetica-Bold", fontSize=12, fillColor=PRIMARY_COLOR))

    if not data:
        return d

    max_val = max(v for _, v in data) or 1
    left = 170
    chart_w = width - left - 60
    row_h = (height - 40) / len(data)

    for i, (label, val) in enumerate(data):
        y = height - 40 - (i + 1) * row_h
        bar_w = (val / max_val) * chart_w

        d.add(String(0, y + 4, label[:30], fontSize=9))
        d.add(Rect(left, y, bar_w, row_h * 0.6, fillColor=PRIMARY_COLOR, strokeColor=None))
        d.add(String(left + chart_w + 6, y + 4, f"{val:.1f}u", fontSize=9))

    return d


# ===============================
# HEADER / FOOTER
# ===============================
def draw_header_footer(canvas, doc):
    canvas.saveState()

    if LOGO_PATH.exists():
        canvas.drawImage(str(LOGO_PATH), 36, A4[1] - 50, width=120, mask="auto")

    canvas.setStrokeColor(PRIMARY_COLOR)
    canvas.setLineWidth(2)
    canvas.line(36, A4[1] - 60, A4[0] - 36, A4[1] - 60)

    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(SECONDARY_COLOR)
    canvas.drawString(36, 28, f"Prolixia • {datetime.now().strftime('%d-%m-%Y')}")
    canvas.drawRightString(A4[0] - 36, 28, f"Pagina {doc.page}")

    canvas.restoreState()


# ===============================
# PDF GENERATIE
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUTPUT_PDF), pagesize=A4, rightMargin=36, leftMargin=36, topMargin=80, bottomMargin=50)

elements = []
elements.append(Paragraph("<b>Support SLA Analyse Rapport</b>", styles["Title"]))
elements.append(Spacer(1, 12))

elements.append(Paragraph(f"<b>Aantal tickets:</b> {total_tickets}", styles["Normal"]))
elements.append(Paragraph(f"<b>Gem. wachttijd per ticket:</b> {avg_wait_per_ticket:.2f} uur", styles["Normal"]))
elements.append(Paragraph(f"<b>Tickets boven SLA (24u):</b> {sla_breach_pct:.1f}%", styles["Normal"]))
elements.append(Spacer(1, 12))

if eur_per_hour > 0:
    elements.append(Paragraph(f"<b>Geschatte maandkosten:</b> €{monthly_loss:,.0f}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Potentiële besparing (20%):</b> €{potential_saving:,.0f}", styles["Normal"]))
    elements.append(Spacer(1, 16))

elements.append(Paragraph("<b>Top SLA-veroorzakers</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

chart = make_bar_chart(list(top_steps.items()), "Gemiddelde wachttijd per stap (uren)")
elements.append(DrawingFlowable(chart))
elements.append(Spacer(1, 16))

elements.append(Paragraph("<b>Top procesknelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if not summary.empty:
    table_data = [["Stap", "Aantal", "Impact (uur)", "Impact (€)"]]
    for _, r in summary.iterrows():
        table_data.append([
            r["event"],
            int(r["occurrences"]),
            f"{r['total_impact_hours']:.2f}",
            f"€{r['total_impact_eur']:.0f}",
        ])

    table = Table(table_data)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("FONT", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
    ]))
    elements.append(table)

doc.build(
    elements,
    onFirstPage=draw_header_footer,
    onLaterPages=draw_header_footer,
)

# ===============================
# METRICS
# ===============================
METRICS_PATH.write_text(json.dumps({
    "created_at": datetime.now(timezone.utc).isoformat(),
    "total_tickets": total_tickets,
    "avg_wait_per_ticket": avg_wait_per_ticket,
    "sla_breach_pct": sla_breach_pct,
    "monthly_loss": monthly_loss,
    "potential_saving": potential_saving,
    "pdf": OUTPUT_PDF.name
}, indent=2))
