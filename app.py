from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import os
import time
import glob
import json
import hmac
import hashlib
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import stripe

app = FastAPI(title="Process Detector")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
TEMPLATES_DIR = BASE_DIR / "templates"
DATA_DIR = BASE_DIR / "data"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# =========================
# STRIPE CONFIG (Render env vars)
# =========================
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC", "").strip()
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO", "").strip()
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").strip()

TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me-please").strip()

BASIC_MONTHLY_UPLOAD_LIMIT = int(os.getenv("BASIC_MONTHLY_UPLOAD_LIMIT", "5"))
PRO_MONTHLY_UPLOAD_LIMIT = int(os.getenv("PRO_MONTHLY_UPLOAD_LIMIT", "999999"))

MAX_HISTORY_ITEMS = int(os.getenv("MAX_HISTORY_ITEMS", "20"))

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

TENANTS_FILE = DATA_DIR / "tenants.json"


# =========================
# Storage helpers
# =========================
def _load_tenants() -> dict:
    if not TENANTS_FILE.exists():
        return {}
    try:
        return json.loads(TENANTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_tenants(data: dict) -> None:
    TENANTS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# Cookie token helpers
# =========================
def _sign_token(raw: str) -> str:
    sig = hmac.new(TOKEN_SIGNING_SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{raw}.{sig}"


def _verify_token(token: str) -> Optional[str]:
    if not token or "." not in token:
        return None
    raw, sig = token.rsplit(".", 1)
    expected = hmac.new(TOKEN_SIGNING_SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
    if hmac.compare_digest(sig, expected):
        return raw
    return None


def _get_auth_email(request: Request) -> Optional[str]:
    token = request.cookies.get("pd_token")
    return _verify_token(token) if token else None


# =========================
# Subscription + Usage helpers
# =========================
def _current_month_key() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _tenant(email: str) -> Dict[str, Any]:
    return (_load_tenants().get(email) or {}) if email else {}


def _is_active_subscriber(email: str) -> bool:
    info = _tenant(email)
    return bool(info.get("active") is True)


def _plan_for(email: str) -> str:
    plan = (_tenant(email).get("plan") or "basic").lower()
    return "pro" if plan == "pro" else "basic"


def _limit_for_plan(plan: str) -> int:
    return PRO_MONTHLY_UPLOAD_LIMIT if plan == "pro" else BASIC_MONTHLY_UPLOAD_LIMIT


def _ensure_month_reset(email: str) -> None:
    tenants = _load_tenants()
    info = tenants.get(email, {}) or {}

    month_key = _current_month_key()
    if info.get("usage_month") != month_key:
        info["usage_month"] = month_key
        info["uploads_this_month"] = 0
        tenants[email] = info
        _save_tenants(tenants)


def _uploads_used(email: str) -> int:
    return int(_tenant(email).get("uploads_this_month", 0) or 0)


def _can_upload(email: str) -> bool:
    _ensure_month_reset(email)
    plan = _plan_for(email)
    used = _uploads_used(email)
    return used < _limit_for_plan(plan)


def _increment_upload(email: str) -> None:
    tenants = _load_tenants()
    info = tenants.get(email, {}) or {}
    _ensure_month_reset(email)
    tenants = _load_tenants()
    info = tenants.get(email, {}) or {}
    info["uploads_this_month"] = int(info.get("uploads_this_month", 0) or 0) + 1
    tenants[email] = info
    _save_tenants(tenants)


# =========================
# PDF helpers
# =========================
def _safe_pdf_name(name: str) -> str:
    name = os.path.basename(name)
    if not name.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Ongeldige bestandsnaam.")
    return name


def _latest_pdf_in_uploads(since_ts: float) -> Path | None:
    pdfs = []
    for p in glob.glob(str(UPLOAD_DIR / "*.pdf")):
        try:
            mtime = os.path.getmtime(p)
            if mtime >= since_ts:
                pdfs.append((mtime, Path(p)))
        except OSError:
            continue
    if not pdfs:
        return None
    pdfs.sort(key=lambda x: x[0], reverse=True)
    return pdfs[0][1]


def _read_last_metrics() -> Dict[str, Any]:
    p = UPLOAD_DIR / "last_metrics.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _append_history(email: str, entry: Dict[str, Any]) -> None:
    tenants = _load_tenants()
    info = tenants.get(email, {}) or {}
    history: List[Dict[str, Any]] = info.get("history", []) or []

    history.append(entry)
    # keep last N
    if len(history) > MAX_HISTORY_ITEMS:
        history = history[-MAX_HISTORY_ITEMS:]

    info["history"] = history
    tenants[email] = info
    _save_tenants(tenants)


# =========================
# Pages
# =========================
@app.get("/landing", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    email = _get_auth_email(request)
    active = bool(email and _is_active_subscriber(email))

    plan = None
    used = None
    limit = None
    history = []

    if email:
        _ensure_month_reset(email)
        plan = _plan_for(email)
        used = _uploads_used(email)
        limit = _limit_for_plan(plan)
        history = (_tenant(email).get("history") or [])[::-1]  # newest first

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "email": email,
            "active": active,
            "plan": plan,
            "used": used,
            "limit": limit,
            "history": history,
        },
    )


# =========================
# STRIPE: Checkout (subscription)
# =========================
@app.post("/billing/checkout")
async def billing_checkout(request: Request):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe niet geconfigureerd (STRIPE_SECRET_KEY ontbreekt).")
    if not STRIPE_PRICE_BASIC or not STRIPE_PRICE_PRO:
        raise HTTPException(status_code=500, detail="Price IDs ontbreken (STRIPE_PRICE_BASIC/PRO).")

    form = await request.form()
    email = (form.get("email") or "").strip().lower()
    plan = (form.get("plan") or "basic").strip().lower()

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Vul een geldig e-mailadres in.")

    if plan == "pro":
        price_id = STRIPE_PRICE_PRO
        plan = "pro"
    else:
        price_id = STRIPE_PRICE_BASIC
        plan = "basic"

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=f"{BASE_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/billing/cancel",
        customer_email=email,
        metadata={"email": email, "plan": plan},
    )
    return RedirectResponse(session.url, status_code=303)


@app.get("/billing/success", response_class=HTMLResponse)
def billing_success(request: Request, session_id: str = ""):
    email = ""
    if session_id:
        try:
            sess = stripe.checkout.Session.retrieve(session_id)
            email = (sess.get("customer_email") or sess.get("metadata", {}).get("email") or "").lower()
        except Exception:
            email = ""

    resp = templates.TemplateResponse("billing_result.html", {"request": request, "ok": True, "email": email})
    if email:
        resp.set_cookie("pd_token", _sign_token(email), httponly=True, samesite="lax")
    return resp


@app.get("/billing/cancel", response_class=HTMLResponse)
def billing_cancel(request: Request):
    return templates.TemplateResponse("billing_result.html", {"request": request, "ok": False, "email": ""})


@app.post("/billing/portal")
async def billing_portal(request: Request):
    email = _get_auth_email(request)
    if not email:
        raise HTTPException(status_code=401, detail="Niet ingelogd.")

    tenants = _load_tenants()
    info = tenants.get(email) or {}
    customer_id = info.get("customer_id")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Geen Stripe customer gevonden voor dit account.")

    portal = stripe.billing_portal.Session.create(customer=customer_id, return_url=f"{BASE_URL}/")
    return RedirectResponse(portal.url, status_code=303)


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret ontbreekt (STRIPE_WEBHOOK_SECRET).")

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")

    tenants = _load_tenants()
    etype = event["type"]
    obj = event["data"]["object"]

    # link email -> customer/subscription + plan
    if etype == "checkout.session.completed":
        email = (obj.get("customer_email") or obj.get("metadata", {}).get("email") or "").lower()
        plan = (obj.get("metadata", {}).get("plan") or "basic").lower()
        plan = "pro" if plan == "pro" else "basic"

        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")

        if email:
            info = tenants.get(email, {}) or {}
            info.update({
                "email": email,
                "plan": plan,
                "customer_id": customer_id,
                "subscription_id": subscription_id,
            })
            tenants[email] = info
            _save_tenants(tenants)

    # subscription status changes
    if etype in ("customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"):
        sub = obj
        customer_id = sub.get("customer")
        status = sub.get("status")
        is_active = status in ("active", "trialing")

        email = None
        for k, v in tenants.items():
            if (v or {}).get("customer_id") == customer_id:
                email = k
                break

        if email:
            info = tenants.get(email, {}) or {}
            info.update({
                "subscription_status": status,
                "active": bool(is_active),
            })
            tenants[email] = info
            _save_tenants(tenants)

    return JSONResponse({"received": True})


# =========================
# CORE: upload + download
# =========================
@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    rate: float = Form(...),  # € per uur
):
    email = _get_auth_email(request)
    if not (email and _is_active_subscriber(email)):
        raise HTTPException(status_code=402, detail="Abonnement vereist. Klik op 'Abonneren' om verder te gaan.")

    _ensure_month_reset(email)

    if not _can_upload(email):
        plan = _plan_for(email)
        used = _uploads_used(email)
        limit = _limit_for_plan(plan)
        raise HTTPException(
            status_code=403,
            detail=f"Uploadlimiet bereikt ({plan.upper()} plan: {used}/{limit} deze maand). Upgrade naar Pro.",
        )

    if not file.filename:
        raise HTTPException(status_code=400, detail="Geen bestand ontvangen.")
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Upload een CSV-bestand (.csv).")

    if rate is None or rate <= 0:
        raise HTTPException(status_code=400, detail="€ per uur moet groter zijn dan 0.")

    # Save CSV to fixed name for analyzer
    csv_path = UPLOAD_DIR / "events.csv"
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Het CSV-bestand is leeg.")
        csv_path.write_bytes(content)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Kon het CSV-bestand niet opslaan.")

    # Unique PDF filename for history
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    pdf_name = f"process_report_{stamp}.pdf"

    started = time.time()
    analyze_script = BASE_DIR / "analyze.py"
    if not analyze_script.exists():
        raise HTTPException(status_code=500, detail="analyze.py niet gevonden op de server.")

    # Run analyzer
    try:
        cmd = [sys.executable, str(analyze_script), str(rate), pdf_name]
        result = subprocess.run(cmd, cwd=str(BASE_DIR), capture_output=True, text=True)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            msg = "Analyse faalde."
            if stderr:
                msg += f" Details: {stderr[:900]}"
            elif stdout:
                msg += f" Output: {stdout[:900]}"
            raise HTTPException(status_code=500, detail=msg)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Er ging iets mis bij het uitvoeren van de analyse.")

    # Find generated pdf
    pdf_path = _latest_pdf_in_uploads(since_ts=started)
    if not pdf_path or not pdf_path.exists():
        raise HTTPException(status_code=500, detail="Analyse klaar, maar geen PDF gevonden in /uploads.")

    # Read metrics & store history
    metrics = _read_last_metrics()
    entry = {
        "date": metrics.get("created_at_utc") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "csv_name": file.filename,
        "pdf": pdf_path.name,
        "eur_per_hour": float(metrics.get("eur_per_hour") or rate),
        "impact_hours": float(metrics.get("total_impact_hours") or 0.0),
        "impact_eur": float(metrics.get("total_impact_eur") or 0.0),
    }
    _append_history(email, entry)

    # Count usage AFTER successful analysis
    _increment_upload(email)

    return JSONResponse({"status": "ok", "filename": pdf_path.name})


@app.get("/download/{filename}")
def download(filename: str):
    filename = _safe_pdf_name(filename)
    pdf_path = UPLOAD_DIR / filename
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF niet gevonden.")
    return FileResponse(path=str(pdf_path), media_type="application/pdf", filename=filename)
