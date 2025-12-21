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

LOGO_PATH = ASSETS_DIR / "logo.png"  # mag ontbreken


# ===============================
# CSV INLEZEN
# ===============================
if not CSV_PATH.exists():
    raise FileNotFoundError("events.csv niet gevonden in /uploads")

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
        f"Ontbrekende kolommen: {missing}. Gevonden: {list(df.columns)}"
    )

df = df.rename(columns={v: k for k, v in normalized.items()})


# ===============================
# CLEAN + SORT
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# PERIODE (voor maand/jaar extrapolatie)
# ===============================
period_start = df["timestamp"].min()
period_end = df["timestamp"].max()
period_hours = (period_end - period_start).total_seconds() / 3600 if pd.notna(period_start) and pd.notna(period_end) else 0.0

# Guard: als periode te klein/ongeldig is, geen extrapolatie
MIN_PERIOD_HOURS = 1.0
can_extrapolate = period_hours >= MIN_PERIOD_HOURS


# ===============================
# DUUR PER STAP
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (df["next_timestamp"] - df["timestamp"]).dt.total_seconds() / 3600

df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]


# ===============================
# BASELINE + IMPACT
# ===============================
baseline = df.groupby("event")["duration_hours"].median()
df["baseline"] = df["event"].map(baseline)

# impact = duur - baseline (alleen positieve impact telt)
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
# ROI / MANAGEMENT METRICS (NIEUW)
# ===============================
MONTH_HOURS = 30 * 24  # ~30 dagen
FTE_HOURS_PER_MONTH = 160.0

if can_extrapolate:
    monthly_factor = MONTH_HOURS / period_hours
    monthly_hours = total_hours * monthly_factor
    monthly_eur = total_eur * monthly_factor
else:
    monthly_factor = 0.0
    monthly_hours = 0.0
    monthly_eur = 0.0

yearly_hours = monthly_hours * 12 if can_extrapolate else 0.0
yearly_eur = monthly_eur * 12 if can_extrapolate else 0.0

fte_equivalent = (monthly_hours / FTE_HOURS_PER_MONTH) if can_extrapolate else 0.0

# (Optioneel) “realistische verbetering” 20%
improve_pct = 0.20
potential_saving_hours = monthly_hours * improve_pct if can_extrapolate else 0.0
potential_saving_eur = monthly_eur * improve_pct if can_extrapolate else 0.0


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
    "Dit rapport toont structurele wachttijden en SLA-overtredingen op basis van support event-logs.",
    styles["Normal"]
))
elements.append(Spacer(1, 12))

# Periode info
if pd.notna(period_start) and pd.notna(period_end):
    elements.append(Paragraph(
        f"<b>Analyseperiode:</b> {period_start.strftime('%d-%m-%Y %H:%M')} t/m {period_end.strftime('%d-%m-%Y %H:%M')}",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 6))

elements.append(Paragraph(
    f"<b>Totale impact:</b> {total_hours:.2f} uur"
    + (f" (€{total_eur:,.2f})" if eur_per_hour > 0 else ""),
    styles["Normal"]
))
elements.append(Spacer(1, 16))

# ===============================
# MANAGEMENTSAMENVATTING (NIEUW)
# ===============================
elements.append(Paragraph("<b>Managementsamenvatting</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if can_extrapolate:
    elements.append(Paragraph(
        f"• Geschatte maandimpact: <b>{monthly_hours:,.1f} uur</b>"
        + (f" (≈ <b>€{monthly_eur:,.0f}</b>)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        f"• FTE-equivalent: <b>{fte_equivalent:.2f} FTE</b> (op basis van 160 uur/maand)",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        f"• Jaarimpact (12 maanden): <b>{yearly_hours:,.0f} uur</b>"
        + (f" (≈ <b>€{yearly_eur:,.0f}</b>)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(
        f"• Potentiële besparing bij 20% verbetering: <b>{potential_saving_hours:,.1f} uur/maand</b>"
        + (f" (≈ <b>€{potential_saving_eur:,.0f}</b>)" if eur_per_hour > 0 else ""),
        styles["Normal"]
    ))
else:
    elements.append(Paragraph(
        "• Extrapolatie naar maand/jaar is niet mogelijk omdat de analyseperiode te klein of onduidelijk is.",
        styles["Normal"]
    ))

elements.append(Spacer(1, 18))

# ===============================
# TOP KNELPUNTEN TABEL
# ===============================
elements.append(Paragraph("<b>Top knelpunten</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if summary.empty:
    elements.append(Paragraph("Geen significante vertragingen gevonden.", styles["Normal"]))
else:
    table_data = [["Stap", "Aantal", "Impact (uur)"]]
    for _, r in summary.iterrows():
        table_data.append([
            str(r["event"]),
            int(r["count"]),
            f"{float(r['total_hours']):.2f}",
        ])

    table = Table(table_data, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))
    elements.append(table)


def header_footer(canvas, doc_):
    canvas.saveState()

    # Logo (optioneel, mag nooit crashen)
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
        f"Pagina {doc_.page}"
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
    "eur_per_hour": eur_per_hour,
    "period_start": period_start.isoformat() if pd.notna(period_start) else None,
    "period_end": period_end.isoformat() if pd.notna(period_end) else None,
    "period_hours": period_hours,
    "total_hours": total_hours,
    "total_eur": total_eur,
    "monthly_hours_est": monthly_hours,
    "monthly_eur_est": monthly_eur,
    "yearly_hours_est": yearly_hours,
    "yearly_eur_est": yearly_eur,
    "fte_equivalent": fte_equivalent,
    "potential_saving_hours": potential_saving_hours,
    "potential_saving_eur": potential_saving_eur,
    "pdf": OUTPUT_PDF.name,
}

METRICS_PATH.write_text(
    json.dumps(metrics, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

print("PDF gegenereerd:", OUTPUT_PDF)
print("Metrics opgeslagen:", METRICS_PATH)
