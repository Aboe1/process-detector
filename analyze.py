import sys
import json
import shutil
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
from reportlab.platypus.flowables import Flowable


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
tenant_key = sys.argv[3] if len(sys.argv) > 3 else "demo"

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
ASSETS_DIR = BASE_DIR / "assets"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / output_pdf_name

LAST_METRICS_PATH = UPLOAD_DIR / "last_metrics.json"
PREV_METRICS_PATH = UPLOAD_DIR / "previous_metrics.json"
HISTORY_PATH = DATA_DIR / "metrics_history.json"

LOGO_PATH = ASSETS_DIR / "logo.png"  # optional


# ===============================
# HELPERS
# ===============================
def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_roll_metrics():
    if LAST_METRICS_PATH.exists():
        try:
            shutil.copyfile(LAST_METRICS_PATH, PREV_METRICS_PATH)
        except Exception:
            pass


def _format_eur(x):
    try:
        return f"€{float(x):,.0f}".replace(",", ".")
    except Exception:
        return "€0"


def _format_pct(x):
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except Exception:
        return "0,0%"


def _load_history():
    data = _read_json(HISTORY_PATH)
    return data if isinstance(data, list) else []


def _save_history(items: list):
    _write_json(HISTORY_PATH, items)


# ===============================
# SLA TYPE MAPPING
# ===============================
def map_sla_type(event: str) -> str:
    s = (event or "").lower()
    if "waiting" in s:
        return "waiting"
    if "resolved" in s or "closed" in s:
        return "resolution"
    if "assigned" in s or "created" in s or "response" in s or "triage" in s:
        return "first_response"
    return "other"


# ===============================
# AI ADVICE (rule-based)
# ===============================
AI_RULES = {
    "first_response": {
        "title": "Versnel eerste reactie",
        "actions": [
            "Stel een SLA in op eerste reactie (< 2 uur)",
            "Activeer automatische ticket-toewijzing",
            "Monitor piekmomenten per kanaal en bemensing",
        ],
        "expected_reduction_pct": 0.25,
    },
    "resolution": {
        "title": "Verkort oplostijd",
        "actions": [
            "Introduceer escalatieregels na 24 uur",
            "Splits complexe tickets in subcases",
            "Analyseer herhaalproblemen (root-cause) en maak fixes structureel",
        ],
        "expected_reduction_pct": 0.30,
    },
    "waiting": {
        "title": "Beperk wachttijd",
        "actions": [
            "Pauzeer SLA bij wachten op klant (contractueel vastleggen)",
            "Stuur automatische reminders na 24/48 uur",
            "Sluit inactieve tickets automatisch na X dagen (met waarschuwing)",
        ],
        "expected_reduction_pct": 0.40,
    },
}


def generate_ai_advice(sla_by_type: dict) -> list:
    # pak top types op basis van risico
    items = []
    for t, v in (sla_by_type or {}).items():
        items.append((t, float(v.get("monthly_risk_eur_est", 0.0) or 0.0)))
    items.sort(key=lambda x: x[1], reverse=True)

    advice = []
    for t, risk in items:
        if risk <= 0:
            continue
        rule = AI_RULES.get(t)
        if not rule:
            continue
        reduction = risk * float(rule["expected_reduction_pct"])
        advice.append(
            {
                "sla_type": t,
                "title": rule["title"],
                "actions": rule["actions"],
                "expected_reduction_pct": float(rule["expected_reduction_pct"]),
                "monthly_risk_reduction_est": round(reduction, 0),
            }
        )
    return advice[:3]


# ===============================
# UPGRADE SIGNALS (urgentie)
# ===============================
COMPLIANCE_WARN_BELOW = 90.0
RISK_WARN_ABOVE_EUR = 1000.0
NEED_2_CONSECUTIVE_DECLINES = True


def build_upgrade_signals(history: list, sla_by_type: dict) -> list:
    signals = []

    # 1) Compliance te laag (per type)
    for t, v in (sla_by_type or {}).items():
        comp = float(v.get("compliance_pct", 0.0) or 0.0)
        if comp < COMPLIANCE_WARN_BELOW:
            severity = "high" if comp < 80 else "medium"
            signals.append(
                {
                    "type": "low_compliance",
                    "severity": severity,
                    "sla_type": t,
                    "message": f"{t.replace('_',' ').title()} compliance is {comp:.1f}% (onder {COMPLIANCE_WARN_BELOW:.0f}%).",
                }
            )

    # 2) Risico te hoog (per type)
    for t, v in (sla_by_type or {}).items():
        risk = float(v.get("monthly_risk_eur_est", 0.0) or 0.0)
        if risk >= RISK_WARN_ABOVE_EUR:
            severity = "high" if risk >= 10000 else "medium"
            signals.append(
                {
                    "type": "high_risk",
                    "severity": severity,
                    "sla_type": t,
                    "message": f"{t.replace('_',' ').title()} risico is ~{_format_eur(risk)}/maand.",
                }
            )

    # 3) Negatieve trend (2x achter elkaar dalend) — per type
    if NEED_2_CONSECUTIVE_DECLINES and len(history) >= 3:
        last3 = history[-3:]
        # verzamel per type compliance reeks
        types = set()
        for h in last3:
            for k in (h.get("sla_by_type") or {}).keys():
                types.add(k)

        for t in types:
            c = []
            for h in last3:
                v = (h.get("sla_by_type") or {}).get(t)
                c.append(float(v.get("compliance_pct", 0.0) or 0.0) if v else None)
            if None in c:
                continue
            # 2 declines op rij: c0 > c1 > c2
            if c[0] > c[1] > c[2]:
                signals.append(
                    {
                        "type": "declining_trend",
                        "severity": "high",
                        "sla_type": t,
                        "message": f"{t.replace('_',' ').title()} compliance daalt 2 metingen op rij ({c[0]:.1f}% → {c[1]:.1f}% → {c[2]:.1f}%).",
                    }
                )

    # sorteer op severity (high eerst) en dan op risico/compliance impliciet via volgorde hierboven
    sev_rank = {"high": 0, "medium": 1, "low": 2}
    signals.sort(key=lambda s: sev_rank.get(s.get("severity", "low"), 9))

    # cap voor UI/ PDF (kort houden)
    return signals[:5]


# ===============================
# VISUALISATIE HELPERS
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


def make_line_chart(points, title, width=520, height=260, suffix="", fmt="{:.1f}"):
    """
    points: list of (label, value)
    """
    d = Drawing(width, height)
    d.add(String(0, height - 16, title, fontName="Helvetica-Bold", fontSize=13, fillColor=colors.HexColor("#0f172a")))

    if len(points) < 2:
        d.add(String(0, height - 40, "Nog onvoldoende data voor trendgrafiek.", fontName="Helvetica", fontSize=10))
        return d

    vals = [float(v) for _, v in points]
    min_v, max_v = min(vals), max(vals)
    if min_v == max_v:
        max_v += 1.0

    left, bottom = 60, 40
    right, top = width - 20, height - 50
    step = (right - left) / (len(points) - 1)

    prev = None
    for i, (label, val) in enumerate(points):
        val = float(val)
        x = left + i * step
        y = bottom + (val - min_v) / (max_v - min_v) * (top - bottom)

        d.add(String(x - 10, bottom - 15, label, fontName="Helvetica", fontSize=8))
        d.add(String(x - 10, y + 6, fmt.format(val) + suffix, fontName="Helvetica", fontSize=8))

        if prev:
            d.add(Rect(prev[0], prev[1], x - prev[0], 1.5, fillColor=colors.HexColor("#2563eb"), strokeColor=None))
        prev = (x, y)

    return d


# ===============================
# PDF HEADER/FOOTER
# ===============================
def header_footer(canvas, doc):
    canvas.saveState()
    if LOGO_PATH.exists():
        try:
            canvas.drawImage(str(LOGO_PATH), 36, A4[1] - 50, width=120, preserveAspectRatio=True, mask="auto")
        except Exception:
            pass

    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(colors.grey)
    canvas.drawString(36, 28, f"Prolixia • {datetime.now().strftime('%d-%m-%Y')}")
    canvas.drawRightString(A4[0] - 36, 28, f"Pagina {doc.page}")
    canvas.restoreState()


# ===============================
# CSV LOAD + NORMALIZE
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
    raise ValueError(f"Ontbrekende kolommen: {missing}. Gevonden: {list(df.columns)}")

df = df.rename(columns={v: k for k, v in normalized.items()})

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])

period_start = df["timestamp"].min()
period_end = df["timestamp"].max()
period_hours = 0.0
if pd.notna(period_start) and pd.notna(period_end):
    period_hours = (period_end - period_start).total_seconds() / 3600.0

can_extrapolate = period_hours >= 1.0

df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (df["next_timestamp"] - df["timestamp"]).dt.total_seconds() / 3600.0
df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]

# ===============================
# BASELINE + DELAYS
# ===============================
baseline = df.groupby("event")["duration_hours"].median().rename("baseline_hours")
df = df.join(baseline, on="event")

df["impact_hours"] = (df["duration_hours"] - df["baseline_hours"]).clip(lower=0)
df["impact_eur"] = df["impact_hours"] * eur_per_hour if eur_per_hour > 0 else 0.0

# Delay: structureel afwijkend (simple)
df["is_delay"] = df["duration_hours"] > 1.5 * df["baseline_hours"]
delays = df[df["is_delay"]].copy()

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

# ===============================
# SLA PER TYPE + TRENDS
# ===============================
# SLA breach: duration > baseline * 1.2 (simple baseline SLA)
df["sla_type"] = df["event"].astype(str).apply(map_sla_type)
df["sla_breach"] = df["duration_hours"] > (df["baseline_hours"] * 1.2)

sla_by_type = {}
for t in ["first_response", "resolution", "waiting"]:
    sub = df[df["sla_type"] == t]
    if sub.empty:
        continue

    steps = int(len(sub))
    breaches = int(sub["sla_breach"].sum())
    compliance = 100.0 * (steps - breaches) / steps if steps > 0 else 0.0

    risk_period = float(sub.loc[sub["sla_breach"], "impact_eur"].sum()) if eur_per_hour > 0 else 0.0
    monthly_factor = (30 * 24 / period_hours) if can_extrapolate else 0.0
    monthly_risk = (risk_period * monthly_factor) if (eur_per_hour > 0 and can_extrapolate) else risk_period

    sla_by_type[t] = {
        "steps": steps,
        "breaches": breaches,
        "compliance_pct": round(compliance, 1),
        "monthly_risk_eur_est": round(monthly_risk, 0),
    }

# history append
history = _load_history()
entry = {
    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "tenant": tenant_key,
    "period": {
        "start": period_start.isoformat() if pd.notna(period_start) else None,
        "end": period_end.isoformat() if pd.notna(period_end) else None,
        "hours": period_hours,
    },
    "sla_by_type": sla_by_type,
}
history.append(entry)
_save_history(history)

# trend by type (last vs previous)
sla_trend_by_type = {}
if len(history) >= 2:
    prev = history[-2].get("sla_by_type") or {}
    curr = history[-1].get("sla_by_type") or {}
    for t, v in curr.items():
        if t not in prev:
            continue
        sla_trend_by_type[t] = {
            "compliance_delta_pp": round(float(v.get("compliance_pct", 0.0)) - float(prev[t].get("compliance_pct", 0.0)), 1),
            "risk_delta_eur": round(float(v.get("monthly_risk_eur_est", 0.0)) - float(prev[t].get("monthly_risk_eur_est", 0.0)), 0),
        }

# build upgrade signals + ai advice
upgrade_signals = build_upgrade_signals(history, sla_by_type)
ai_advice = generate_ai_advice(sla_by_type)

# ===============================
# METRICS SAVE
# ===============================
_safe_roll_metrics()

current_metrics = {
    "generated_at": entry["generated_at"],
    "tenant": tenant_key,
    "period": entry["period"],
    "sla_by_type": sla_by_type,
    "sla_trend_by_type": sla_trend_by_type,
    "upgrade_signals": upgrade_signals,
    "ai_advice": ai_advice,
    "pdf": OUTPUT_PDF.name,
}
_write_json(LAST_METRICS_PATH, current_metrics)

# ===============================
# PDF GENERATION
# ===============================
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(
    str(OUTPUT_PDF),
    pagesize=A4,
    rightMargin=36,
    leftMargin=36,
    topMargin=72,
    bottomMargin=36,
)

elements = []

elements.append(Paragraph("<b>Prolixia – Support SLA Analyse</b>", styles["Title"]))
elements.append(Spacer(1, 10))

if pd.notna(period_start) and pd.notna(period_end):
    elements.append(Paragraph(
        f"<b>Analyseperiode:</b> {period_start.strftime('%d-%m-%Y %H:%M')} t/m {period_end.strftime('%d-%m-%Y %H:%M')}",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 6))

# Executive summary (per type)
elements.append(Paragraph("<b>Executive SLA Intelligence (per type)</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if not sla_by_type:
    elements.append(Paragraph("Geen SLA-type data beschikbaar.", styles["Normal"]))
else:
    rows = [["SLA-type", "Steps", "Breaches", "Compliance", "Risico/maand (€)"]]
    for t, v in sla_by_type.items():
        rows.append([
            t.replace("_", " ").title(),
            int(v["steps"]),
            int(v["breaches"]),
            f"{float(v['compliance_pct']):.1f}",
            f"{float(v['monthly_risk_eur_est']):,.0f}".replace(",", "."),
        ])
    table = Table(rows, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("FONT", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ("BOTTOMPADDING", (0,0), (-1,0), 10),
    ]))
    elements.append(table)

elements.append(Spacer(1, 14))

# Upgrade urgency section
elements.append(Paragraph("<b>⚠️ Actie vereist (upgrade signals)</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
if not upgrade_signals:
    elements.append(Paragraph("Geen urgente signalen gedetecteerd.", styles["Normal"]))
else:
    for s in upgrade_signals:
        elements.append(Paragraph(f"• {s['message']}", styles["Normal"]))
elements.append(Spacer(1, 12))

# AI advice
elements.append(Paragraph("<b>AI-gestuurde verbeteracties</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
if not ai_advice:
    elements.append(Paragraph("Geen AI-advies beschikbaar (onvoldoende risico/breaches).", styles["Normal"]))
else:
    for item in ai_advice:
        elements.append(Paragraph(f"<b>{item['title']}</b>", styles["Normal"]))
        elements.append(Paragraph(
            f"Verwachte risicoreductie: <b>{_format_eur(item['monthly_risk_reduction_est'])} / maand</b>",
            styles["Normal"]
        ))
        for act in item["actions"]:
            elements.append(Paragraph(f"• {act}", styles["Normal"]))
        elements.append(Spacer(1, 6))

# Visual page: trend charts per type
elements.append(PageBreak())
elements.append(Paragraph("<b>SLA trends per type</b>", styles["Title"]))
elements.append(Spacer(1, 12))

# Build points from history (last 6)
hist_last = history[-6:]
for t in ["first_response", "resolution", "waiting"]:
    pts = []
    for i, h in enumerate(hist_last):
        v = (h.get("sla_by_type") or {}).get(t)
        if not v:
            continue
        pts.append((f"T{i+1}", float(v.get("compliance_pct", 0.0) or 0.0)))

    elements.append(Paragraph(f"<b>{t.replace('_',' ').title()}</b>", styles["Heading2"]))
    elements.append(Spacer(1, 6))
    elements.append(DrawingFlowable(make_line_chart(pts, f"Compliance trend — {t.replace('_',' ')}", suffix="%", fmt="{:.1f}")))
    elements.append(Spacer(1, 14))

doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)

print(f"PDF gegenereerd: {OUTPUT_PDF}")
print(f"Metrics saved: {LAST_METRICS_PATH} | History: {HISTORY_PATH}")
