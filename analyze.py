import sys
import json
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
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
tenant_key = sys.argv[3] if len(sys.argv) > 3 else "demo"  # email or "demo"

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
ASSETS_DIR = BASE_DIR / "assets"
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = UPLOAD_DIR / "events.csv"
OUTPUT_PDF = UPLOAD_DIR / output_pdf_name

LAST_METRICS_PATH = UPLOAD_DIR / "last_metrics.json"
PREV_METRICS_PATH = UPLOAD_DIR / "previous_metrics.json"

LOGO_PATH = ASSETS_DIR / "logo.png"  # optional
SLA_CONFIG_PATH = DATA_DIR / "sla_configs.json"  # optional


# ===============================
# SLA SETTINGS (baseline fallback)
# ===============================
SLA_TARGET_MULTIPLIER = 1.20
SLA_MIN_TARGET_HOURS = 0.05
DEFAULT_RISK_FACTOR = 1.25

RISK_FACTOR_KEYWORDS = [
    ("escalat", 2.0),
    ("urgent", 2.0),
    ("priority", 1.8),
    ("p1", 1.8),
    ("p0", 2.0),
    ("security", 2.2),
    ("outage", 2.2),
    ("incident", 1.8),
    ("waiting for customer", 1.1),
    ("waiting", 1.1),
    ("customer", 1.1),
    ("internal", 1.3),
    ("queue", 1.3),
]

# ===============================
# SLA TYPES + event mapping
# ===============================
EVENT_TO_SLA_TYPE = {
    "assigned": "first_response",
    "agent response": "first_response",
    "triage": "first_response",
    "ticket created": "first_response",
    "created": "first_response",
    "resolved": "resolution",
    "closed": "resolution",
    "waiting for customer": "waiting_for_customer",
    "waiting for internal team": "waiting_for_internal_team",
}

AI_ADVICE_RULES = {
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
    "waiting_for_customer": {
        "title": "Beperk wachttijd bij klant",
        "actions": [
            "Pauzeer SLA bij wachten op klant (contractueel vastleggen)",
            "Stuur automatische reminders na 24/48 uur",
            "Sluit inactieve tickets automatisch na X dagen (met waarschuwing)",
        ],
        "expected_reduction_pct": 0.40,
    },
    "waiting_for_internal_team": {
        "title": "Beperk interne wachtrijen",
        "actions": [
            "Maak ownership per queue expliciet (RACI)",
            "Introduceer WIP-limieten per team",
            "Automatiseer routing naar juiste team op basis van categorie",
        ],
        "expected_reduction_pct": 0.25,
    },
}


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


def _write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_roll_metrics():
    if LAST_METRICS_PATH.exists():
        try:
            shutil.copyfile(LAST_METRICS_PATH, PREV_METRICS_PATH)
        except Exception:
            pass


def _pct_change(curr, prev):
    try:
        curr = float(curr)
        prev = float(prev)
        if prev == 0:
            return None
        return (curr - prev) / prev * 100.0
    except Exception:
        return None


def _format_eur(x):
    try:
        return f"€{float(x):,.0f}".replace(",", ".")
    except Exception:
        return "€0"


def _format_hours(x):
    try:
        return f"{float(x):,.1f} uur".replace(",", ".")
    except Exception:
        return "0.0 uur"


def _format_fte(x):
    try:
        return f"{float(x):.2f} FTE"
    except Exception:
        return "0.00 FTE"


def _format_pct(x):
    try:
        return f"{float(x):.1f}%".replace(".", ",")
    except Exception:
        return "0,0%"


def risk_factor_for_event(event_name: str) -> float:
    s = (event_name or "").strip().lower()
    for key, factor in RISK_FACTOR_KEYWORDS:
        if key in s:
            return float(factor)
    return float(DEFAULT_RISK_FACTOR)


def _map_event_to_sla_type(ev: str) -> str | None:
    s = (ev or "").strip().lower()
    for key, sla_type in EVENT_TO_SLA_TYPE.items():
        if key in s:
            return sla_type
    return None


# ===============================
# SLA CONFIG (tenant)
# ===============================
def load_sla_configs() -> dict:
    data = _read_json(SLA_CONFIG_PATH)
    return data if isinstance(data, dict) else {}


def get_tenant_config(tenant: str) -> dict | None:
    cfgs = load_sla_configs()
    if tenant in cfgs:
        return cfgs.get(tenant)
    if "demo" in cfgs:
        return cfgs.get("demo")
    return None


def get_sla_rule(tenant_cfg: dict | None, sla_type: str | None) -> dict | None:
    if not tenant_cfg or not sla_type:
        return None
    rules = tenant_cfg.get("sla", {}) or {}
    return rules.get(sla_type)


# ===============================
# AI ADVICE
# ===============================
def generate_ai_advice(sla_metrics: dict) -> list[dict]:
    """
    Rule-based advice. Requires sla_metrics['breaches_by_type'] + monthly risk.
    """
    advice = []
    breaches_by_type = sla_metrics.get("breaches_by_type", {}) or {}
    monthly_risk = float(sla_metrics.get("monthly_risk_eur_est", 0.0) or 0.0)

    # sort by most breaches first
    items = sorted(breaches_by_type.items(), key=lambda kv: kv[1], reverse=True)

    for sla_type, breaches in items:
        if breaches <= 0:
            continue
        rule = AI_ADVICE_RULES.get(sla_type)
        if not rule:
            continue

        pct = float(rule.get("expected_reduction_pct", 0.2))
        reduction = monthly_risk * pct if monthly_risk > 0 else 0.0

        advice.append({
            "sla_type": sla_type,
            "title": rule["title"],
            "actions": rule["actions"],
            "expected_reduction_pct": pct,
            "monthly_risk_reduction_est": round(reduction, 0),
        })

    # keep top 3 for report clarity
    return advice[:3]


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


# ===============================
# CLEAN + SORT
# ===============================
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp", "case_id", "event"])
df = df.sort_values(["case_id", "timestamp"])


# ===============================
# PERIOD
# ===============================
period_start = df["timestamp"].min()
period_end = df["timestamp"].max()
period_hours = 0.0
if pd.notna(period_start) and pd.notna(period_end):
    period_hours = (period_end - period_start).total_seconds() / 3600.0

MIN_PERIOD_HOURS = 1.0
can_extrapolate = period_hours >= MIN_PERIOD_HOURS


# ===============================
# DURATIONS
# ===============================
df["next_timestamp"] = df.groupby("case_id")["timestamp"].shift(-1)
df["duration_hours"] = (df["next_timestamp"] - df["timestamp"]).dt.total_seconds() / 3600.0
df = df.dropna(subset=["duration_hours"])
df = df[df["duration_hours"] >= 0]


# ===============================
# BASELINE + DELAYS (process impact)
# ===============================
baseline = df.groupby("event")["duration_hours"].median().rename("baseline_hours")
df = df.join(baseline, on="event")

df["is_delay"] = df["duration_hours"] > 1.5 * df["baseline_hours"]
df["impact_hours"] = (df["duration_hours"] - df["baseline_hours"]).clip(lower=0)

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
# SLA (tenant config overrides)
# ===============================
tenant_cfg = get_tenant_config(tenant_key)
currency = (tenant_cfg or {}).get("currency", "EUR")
penalty_model = (tenant_cfg or {}).get("penalty_model", "per_hour")

df["sla_type"] = df["event"].astype(str).apply(_map_event_to_sla_type)
df["sla_target_hours"] = (df["baseline_hours"] * SLA_TARGET_MULTIPLIER).clip(lower=SLA_MIN_TARGET_HOURS)
df["pause_sla"] = False
df["penalty_per_hour"] = 0.0

if tenant_cfg:
    def _apply_rule(row):
        sla_type = row.get("sla_type")
        rule = get_sla_rule(tenant_cfg, sla_type)
        if not rule:
            return row
        if bool(rule.get("pause_sla", False)):
            row["pause_sla"] = True
        if rule.get("target_hours") is not None:
            try:
                row["sla_target_hours"] = max(float(rule["target_hours"]), SLA_MIN_TARGET_HOURS)
            except Exception:
                pass
        if rule.get("penalty_per_hour") is not None:
            try:
                row["penalty_per_hour"] = float(rule["penalty_per_hour"])
            except Exception:
                pass
        return row

    df = df.apply(_apply_rule, axis=1)

df["sla_breach"] = (~df["pause_sla"]) & (df["duration_hours"] > df["sla_target_hours"])
df["sla_over_hours"] = (df["duration_hours"] - df["sla_target_hours"]).clip(lower=0)
df.loc[df["pause_sla"], "sla_over_hours"] = 0.0

total_steps = int(len(df))
total_breaches = int(df["sla_breach"].sum()) if total_steps > 0 else 0
sla_compliance_pct = (100.0 * (total_steps - total_breaches) / total_steps) if total_steps > 0 else 0.0
sla_breach_ratio = (100.0 * total_breaches / total_steps) if total_steps > 0 else 0.0

# breaches by type (for AI)
breaches_by_type = (
    df[df["sla_breach"]]
    .groupby("sla_type")["sla_breach"]
    .count()
    .to_dict()
)
# ensure all keys exist as int
breaches_by_type = {k: int(v) for k, v in breaches_by_type.items() if k}

# Risk (€) (depends on eur_per_hour)
df["risk_factor"] = df["event"].astype(str).apply(risk_factor_for_event)
df["sla_risk_eur"] = 0.0
if eur_per_hour > 0:
    df["sla_risk_eur"] = df["sla_over_hours"] * eur_per_hour * df["risk_factor"]
sla_risk_total_eur = float(df["sla_risk_eur"].sum()) if eur_per_hour > 0 else 0.0

# Penalties (€) (contractual)
df["sla_penalty_eur"] = 0.0
df.loc[df["sla_breach"], "sla_penalty_eur"] = df["sla_over_hours"] * df["penalty_per_hour"]
sla_penalty_total_eur = float(df["sla_penalty_eur"].sum())

sla_by_event = (
    df.groupby("event")
    .agg(
        steps=("event", "count"),
        breaches=("sla_breach", "sum"),
        compliance_pct=("sla_breach", lambda s: 100.0 * (len(s) - float(s.sum())) / max(1, len(s))),
        over_hours=("sla_over_hours", "sum"),
        risk_eur=("sla_risk_eur", "sum"),
        penalty_eur=("sla_penalty_eur", "sum"),
        sla_type=("sla_type", lambda x: (x.dropna().iloc[0] if len(x.dropna()) else None)),
    )
    .reset_index()
)
sla_by_event["breaches"] = sla_by_event["breaches"].astype(int)

top_risk_event, top_risk_eur = (None, 0.0)
if not sla_by_event.empty and eur_per_hour > 0:
    t = sla_by_event.sort_values("risk_eur", ascending=False).iloc[0]
    top_risk_event, top_risk_eur = str(t["event"]), float(t["risk_eur"])

top_penalty_event, top_penalty_eur = (None, 0.0)
if not sla_by_event.empty and sla_penalty_total_eur > 0:
    t = sla_by_event.sort_values("penalty_eur", ascending=False).iloc[0]
    top_penalty_event, top_penalty_eur = str(t["event"]), float(t["penalty_eur"])


# ===============================
# MANAGEMENT METRICS
# ===============================
MONTH_HOURS = 30 * 24
FTE_HOURS_PER_MONTH = 160.0

monthly_factor = (MONTH_HOURS / period_hours) if can_extrapolate else 0.0
monthly_hours_est = total_impact_hours * monthly_factor if can_extrapolate else 0.0
monthly_eur_est = total_impact_eur * monthly_factor if (can_extrapolate and eur_per_hour > 0) else 0.0

yearly_hours_est = monthly_hours_est * 12 if can_extrapolate else 0.0
yearly_eur_est = monthly_eur_est * 12 if can_extrapolate else 0.0

fte_equivalent = (monthly_hours_est / FTE_HOURS_PER_MONTH) if can_extrapolate else 0.0

improve_pct = 0.20
potential_saving_hours = monthly_hours_est * improve_pct if can_extrapolate else 0.0
potential_saving_eur = monthly_eur_est * improve_pct if can_extrapolate else 0.0

monthly_sla_risk_eur_est = (sla_risk_total_eur * monthly_factor) if (can_extrapolate and eur_per_hour > 0) else 0.0
yearly_sla_risk_eur_est = monthly_sla_risk_eur_est * 12 if (can_extrapolate and eur_per_hour > 0) else 0.0

monthly_penalty_eur_est = (sla_penalty_total_eur * monthly_factor) if can_extrapolate else 0.0
yearly_penalty_eur_est = monthly_penalty_eur_est * 12 if can_extrapolate else 0.0


# ===============================
# TOP BOTTLENECK (process delays)
# ===============================
top_bottleneck_event = None
top_bottleneck_hours = 0.0
if not summary.empty:
    top_bottleneck_event = str(summary.iloc[0]["event"])
    top_bottleneck_hours = float(summary.iloc[0]["total_impact_hours"])


# ===============================
# METRICS SAVE + comparison
# ===============================
_safe_roll_metrics()
previous_metrics = _read_json(PREV_METRICS_PATH)

current_metrics = {
    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "tenant": tenant_key,
    "period": {
        "start": period_start.isoformat() if pd.notna(period_start) else None,
        "end": period_end.isoformat() if pd.notna(period_end) else None,
        "hours": period_hours,
    },
    "impact": {
        "total_hours": total_impact_hours,
        "total_eur": total_impact_eur,
        "monthly_hours_est": monthly_hours_est,
        "monthly_eur_est": monthly_eur_est,
        "yearly_hours_est": yearly_hours_est,
        "yearly_eur_est": yearly_eur_est,
        "fte_equivalent": fte_equivalent,
        "potential_saving_hours": potential_saving_hours,
        "potential_saving_eur": potential_saving_eur,
    },
    "sla": {
        "mode": "tenant_config" if tenant_cfg else "baseline_multiplier",
        "currency": currency,
        "penalty_model": penalty_model,
        "target_multiplier": SLA_TARGET_MULTIPLIER,
        "min_target_hours": SLA_MIN_TARGET_HOURS,
        "total_steps": total_steps,
        "total_breaches": total_breaches,
        "breaches_by_type": breaches_by_type,
        "compliance_pct": sla_compliance_pct,
        "breach_ratio_pct": sla_breach_ratio,
        "risk_total_eur": sla_risk_total_eur,
        "monthly_risk_eur_est": monthly_sla_risk_eur_est,
        "yearly_risk_eur_est": yearly_sla_risk_eur_est,
        "top_risk_event": top_risk_event,
        "top_risk_eur": top_risk_eur,
        "penalty_total_eur": sla_penalty_total_eur,
        "monthly_penalty_eur_est": monthly_penalty_eur_est,
        "yearly_penalty_eur_est": yearly_penalty_eur_est,
        "top_penalty_event": top_penalty_event,
        "top_penalty_eur": top_penalty_eur,
    },
    "top_bottleneck": {"event": top_bottleneck_event, "impact_hours": top_bottleneck_hours},
    "pdf": OUTPUT_PDF.name,
}

# AI advice lives at top-level (easy for UI)
current_metrics["ai_advice"] = generate_ai_advice(current_metrics["sla"])

_write_json(LAST_METRICS_PATH, current_metrics)

comparison = None
if previous_metrics and isinstance(previous_metrics, dict):
    prev_sla = previous_metrics.get("sla", {}) or {}
    curr_sla = current_metrics.get("sla", {}) or {}

    prev_comp = prev_sla.get("compliance_pct", None)
    curr_comp = curr_sla.get("compliance_pct", None)
    delta_comp_pp = (float(curr_comp) - float(prev_comp)) if (prev_comp is not None and curr_comp is not None) else None

    prev_risk_m = float(prev_sla.get("monthly_risk_eur_est", 0.0) or 0.0)
    curr_risk_m = float(curr_sla.get("monthly_risk_eur_est", 0.0) or 0.0)
    delta_risk_m = (curr_risk_m - prev_risk_m) if (eur_per_hour > 0 and can_extrapolate) else None

    prev_pen_m = float(prev_sla.get("monthly_penalty_eur_est", 0.0) or 0.0)
    curr_pen_m = float(curr_sla.get("monthly_penalty_eur_est", 0.0) or 0.0)
    delta_pen_m = (curr_pen_m - prev_pen_m) if can_extrapolate else None

    comparison = {
        "delta_compliance_pp": delta_comp_pp,
        "delta_month_risk_eur": delta_risk_m,
        "delta_month_penalty_eur": delta_pen_m,
    }


# ===============================
# VISUALISATION (ReportLab)
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


def make_bar_chart(data, title, width=520, height=360, value_suffix="", value_fmt="{:.1f}"):
    d = Drawing(width, height)
    d.add(String(0, height - 16, title, fontName="Helvetica-Bold", fontSize=13, fillColor=colors.HexColor("#0f172a")))

    if not data:
        d.add(String(0, height - 40, "Geen data beschikbaar.", fontName="Helvetica", fontSize=10))
        return d

    max_val = max(v for _, v in data) if data else 1.0
    if max_val <= 0:
        max_val = 1.0

    left_label = 220
    right_pad = 60
    chart_w = width - left_label - right_pad
    top = height - 44
    row_h = max(24, int((top - 10) / max(1, len(data))))

    y = top - row_h
    for label, val in data:
        bar_w = (float(val) / max_val) * chart_w
        d.add(String(0, y + 7, str(label)[:34], fontName="Helvetica", fontSize=9))
        d.add(Rect(left_label, y + 4, bar_w, 12, fillColor=colors.HexColor("#0f172a"), strokeColor=None))
        try:
            shown = value_fmt.format(float(val)) + value_suffix
        except Exception:
            shown = str(val) + value_suffix
        d.add(String(left_label + chart_w + 6, y + 7, shown, fontName="Helvetica", fontSize=9))
        y -= row_h
        if y < 8:
            break

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
        styles["Normal"],
    ))
    elements.append(Spacer(1, 6))

elements.append(Paragraph(
    f"<b>Totale impact (delays vs baseline):</b> {_format_hours(total_impact_hours)}"
    + (f" (≈ {_format_eur(total_impact_eur)})" if eur_per_hour > 0 else ""),
    styles["Normal"],
))
elements.append(Spacer(1, 10))

# Executive SLA
elements.append(Paragraph("<b>Executive SLA Intelligence</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
elements.append(Paragraph(
    f"• SLA-compliance: <b>{_format_pct(sla_compliance_pct)}</b> (breach ratio: {_format_pct(sla_breach_ratio)})",
    styles["Normal"],
))
elements.append(Spacer(1, 4))

if eur_per_hour > 0 and can_extrapolate:
    elements.append(Paragraph(
        f"• Financiële risico-exposure: <b>{_format_eur(monthly_sla_risk_eur_est)}/maand</b> (≈ {_format_eur(yearly_sla_risk_eur_est)}/jaar)",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 4))

if top_risk_event and eur_per_hour > 0:
    elements.append(Paragraph(
        f"• Grootste SLA-risico processtap: <b>{top_risk_event}</b> ({_format_eur(top_risk_eur)})",
        styles["Normal"],
    ))

elements.append(Spacer(1, 12))

# Contractual exposure
show_penalties = bool(tenant_cfg) and (sla_penalty_total_eur > 0 or monthly_penalty_eur_est > 0)

if show_penalties:
    elements.append(Paragraph("<b>Contractuele SLA-exposure</b>", styles["Heading2"]))
    elements.append(Spacer(1, 8))
    if can_extrapolate:
        elements.append(Paragraph(
            f"• Geschatte boete-exposure: <b>{_format_eur(monthly_penalty_eur_est)}/maand</b> (≈ {_format_eur(yearly_penalty_eur_est)}/jaar)",
            styles["Normal"],
        ))
    else:
        elements.append(Paragraph(
            f"• Boete-exposure in analyseperiode: <b>{_format_eur(sla_penalty_total_eur)}</b>",
            styles["Normal"],
        ))
    if top_penalty_event:
        elements.append(Spacer(1, 4))
        elements.append(Paragraph(
            f"• Grootste contractuele exposure: <b>{top_penalty_event}</b> ({_format_eur(top_penalty_eur)})",
            styles["Normal"],
        ))
else:
    elements.append(Paragraph("<b>Contractuele SLA-exposure (Enterprise)</b>", styles["Heading2"]))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "Contractuele boete-simulatie is beschikbaar in het Enterprise plan (SLA-config + penalty per uur).",
        styles["Normal"],
    ))

elements.append(Spacer(1, 12))

# Comparison
elements.append(Paragraph("<b>Vergelijking met vorige periode</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
if comparison is None:
    elements.append(Paragraph(
        "Dit is de eerste analyse. De volgende analyse wordt automatisch vergeleken met deze nulmeting.",
        styles["Normal"],
    ))
else:
    if comparison.get("delta_compliance_pp") is not None:
        elements.append(Paragraph(
            f"• SLA-compliance verandering: <b>{comparison['delta_compliance_pp']:+.1f}pp</b>",
            styles["Normal"],
        ))
    if comparison.get("delta_month_risk_eur") is not None:
        elements.append(Paragraph(
            f"• Risico-exposure (maand) verschil: <b>{_format_eur(comparison['delta_month_risk_eur'])}</b>",
            styles["Normal"],
        ))
    if comparison.get("delta_month_penalty_eur") is not None:
        elements.append(Paragraph(
            f"• Boete-exposure (maand) verschil: <b>{_format_eur(comparison['delta_month_penalty_eur'])}</b>",
            styles["Normal"],
        ))

elements.append(Spacer(1, 12))

# AI advice
elements.append(Paragraph("<b>AI-gestuurde verbeteracties</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))
ai_items = current_metrics.get("ai_advice", []) or []
if not ai_items:
    elements.append(Paragraph("Geen significante SLA-breaches gedetecteerd voor AI-advies.", styles["Normal"]))
else:
    for item in ai_items:
        elements.append(Paragraph(f"<b>{item['title']}</b>", styles["Normal"]))
        if can_extrapolate and eur_per_hour > 0:
            elements.append(Paragraph(
                f"Verwachte risicoreductie: <b>€{item['monthly_risk_reduction_est']:,.0f} / maand</b>",
                styles["Normal"],
            ))
        for act in item["actions"]:
            elements.append(Paragraph(f"• {act}", styles["Normal"]))
        elements.append(Spacer(1, 6))

elements.append(Spacer(1, 10))

# SLA table
elements.append(Paragraph("<b>SLA compliance, risico & boete per processtap</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if sla_by_event.empty:
    elements.append(Paragraph("Geen SLA-data beschikbaar.", styles["Normal"]))
else:
    table_rows = [["Processtap", "SLA-type", "Steps", "Breaches", "Compliance %", "Over (uur)", "Risico (€)", "Boete (€)"]]
    top_df = sla_by_event.sort_values(["penalty_eur", "risk_eur"], ascending=False).head(12)
    for _, r in top_df.iterrows():
        table_rows.append([
            str(r["event"]),
            str(r["sla_type"] or "-"),
            int(r["steps"]),
            int(r["breaches"]),
            f"{float(r['compliance_pct']):.1f}",
            f"{float(r['over_hours']):.2f}",
            f"{float(r['risk_eur']):,.0f}".replace(",", "."),
            f"{float(r['penalty_eur']):,.0f}".replace(",", "."),
        ])

    table = Table(table_rows, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))
    elements.append(table)

elements.append(Spacer(1, 12))

# Top bottlenecks (process delays)
elements.append(Paragraph("<b>Top knelpunten (delays vs baseline)</b>", styles["Heading2"]))
elements.append(Spacer(1, 8))

if summary.empty:
    elements.append(Paragraph("Geen significante procesvertragingen gedetecteerd.", styles["Normal"]))
else:
    if eur_per_hour > 0:
        table_data = [["Processtap", "Aantal", "Impact (uren)", "Impact (€)"]]
        for _, row in summary.head(12).iterrows():
            table_data.append([
                str(row["event"]),
                int(row["occurrences"]),
                f"{float(row['total_impact_hours']):.2f}",
                f"{float(row['total_impact_eur']):,.0f}".replace(",", "."),
            ])
    else:
        table_data = [["Processtap", "Aantal", "Impact (uren)"]]
        for _, row in summary.head(12).iterrows():
            table_data.append([
                str(row["event"]),
                int(row["occurrences"]),
                f"{float(row['total_impact_hours']):.2f}",
            ])

    t = Table(table_data, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))
    elements.append(t)

# Visualisations page
elements.append(PageBreak())
elements.append(Paragraph("<b>Visualisaties</b>", styles["Title"]))
elements.append(Spacer(1, 14))

# Chart 1: penalties if available, else risk, else hours impact
if not sla_by_event.empty and sla_by_event["penalty_eur"].sum() > 0:
    series = [(str(r["event"]), float(r["penalty_eur"])) for _, r in sla_by_event.sort_values("penalty_eur", ascending=False).head(10).iterrows()]
    chart = make_bar_chart(series, "Contractuele boete-exposure (€) per processtap — Top 10", value_suffix="", value_fmt="{:.0f}")
    elements.append(DrawingFlowable(chart))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph("Deze grafiek toont boete-simulatie op basis van klant-SLA-configuratie.", styles["Normal"]))
elif eur_per_hour > 0 and not sla_by_event.empty and sla_by_event["risk_eur"].sum() > 0:
    series = [(str(r["event"]), float(r["risk_eur"])) for _, r in sla_by_event.sort_values("risk_eur", ascending=False).head(10).iterrows()]
    chart = make_bar_chart(series, "SLA risico (€) per processtap — Top 10", value_suffix="", value_fmt="{:.0f}")
    elements.append(DrawingFlowable(chart))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph("Deze grafiek toont geschatte financiële exposure door SLA-overschrijdingen.", styles["Normal"]))
else:
    # fallback: process delays hours
    series = []
    if not summary.empty:
        for _, row in summary.head(10).iterrows():
            series.append((str(row["event"]), float(row["total_impact_hours"])))
    chart = make_bar_chart(series, "Impact (uren) per processtap — Top 10", value_suffix="u", value_fmt="{:.1f}")
    elements.append(DrawingFlowable(chart))
    elements.append(Spacer(1, 8))
    elements.append(Paragraph("Hoe langer de balk, hoe groter de structurele vertraging in deze stap.", styles["Normal"]))

doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)

print(f"PDF gegenereerd: {OUTPUT_PDF}")
print(f"Metrics saved: {LAST_METRICS_PATH} (previous: {PREV_METRICS_PATH})")
print(f"Tenant: {tenant_key} (config: {'yes' if tenant_cfg else 'no'})")
