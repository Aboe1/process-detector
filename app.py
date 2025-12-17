from fastapi import FastAPI, UploadFile, File, HTTPException
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
from pathlib import Path
from typing import Optional

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
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "").strip()
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").strip()

# Cookie token signing (set this in Render!)
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me-please").strip()

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

TENANTS_FILE = DATA_DIR / "tenants.json"


# =========================
# Tiny storage helpers (JSON file)
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


def _is_active_subscriber(email: str) -> bool:
    tenants = _load_tenants()
    info = tenants.get(email)
    return bool(info and info.get("active") is True)


# =========================
# Upload/download helpers
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


# =========================
# Pages
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    email = _get_auth_email(request)
    active = bool(email and _is_active_subscriber(email))
    return templates.TemplateResponse("index.html", {"request": request, "email": email, "active": active})


# =========================
# STRIPE: Checkout (subscription)
# =========================
@app.post("/billing/checkout")
async def billing_checkout(request: Request):
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        raise HTTPException(status_code=500, detail="Stripe is niet geconfigureerd (env vars ontbreken).")

    form = await request.form()
    email = (form.get("email") or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Vul een geldig e-mailadres in.")

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
        success_url=f"{BASE_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/billing/cancel",
        customer_email=email,
        metadata={"email": email},
    )
    return RedirectResponse(session.url, status_code=303)


@app.get("/billing/success", response_class=HTMLResponse)
def billing_success(request: Request, session_id: str = ""):
    # Cookie zetten na success; "active" wordt definitief via webhook.
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


# =========================
# STRIPE: Customer Portal
# =========================
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

    portal = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=f"{BASE_URL}/",
    )
    return RedirectResponse(portal.url, status_code=303)


# =========================
# STRIPE: Webhook
# =========================
@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret ontbreekt.")

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")

    tenants = _load_tenants()
    etype = event["type"]
    obj = event["data"]["object"]

    # Link email -> customer/subscription
    if etype == "checkout.session.completed":
        email = (obj.get("customer_email") or obj.get("metadata", {}).get("email") or "").lower()
        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")
        if email:
            tenants[email] = tenants.get(email, {})
            tenants[email].update({
                "email": email,
                "customer_id": customer_id,
                "subscription_id": subscription_id,
            })
            _save_tenants(tenants)

    # Subscription status updates
    if etype in ("customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"):
        sub = obj
        customer_id = sub.get("customer")
        status = sub.get("status")  # active, canceled, trialing, etc.
        is_active = status in ("active", "trialing")

        email = None
        for k, v in tenants.items():
            if v.get("customer_id") == customer_id:
                email = k
                break

        if email:
            tenants[email] = tenants.get(email, {})
            tenants[email].update({
                "subscription_status": status,
                "active": bool(is_active),
            })
            _save_tenants(tenants)

    return JSONResponse({"received": True})


# =========================
# Upload (paywalled) + download
# =========================
@app.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    email = _get_auth_email(request)
    if not (email and _is_active_subscriber(email)):
        raise HTTPException(status_code=402, detail="Abonnement vereist. Klik op 'Abonneren' om verder te gaan.")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Geen bestand ontvangen.")
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Upload een CSV-bestand (.csv).")

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

    started = time.time()
    analyze_script = BASE_DIR / "analyze.py"
    if not analyze_script.exists():
        raise HTTPException(status_code=500, detail="analyze.py niet gevonden op de server.")

    try:
        cmd = [sys.executable, str(analyze_script)]
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

    pdf_path = _latest_pdf_in_uploads(since_ts=started)
    if not pdf_path or not pdf_path.exists():
        raise HTTPException(status_code=500, detail="Analyse klaar, maar geen PDF gevonden in /uploads.")

    return JSONResponse({"status": "ok", "filename": pdf_path.name})


@app.get("/download/{filename}")
def download(filename: str):
    filename = _safe_pdf_name(filename)
    pdf_path = UPLOAD_DIR / filename
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF niet gevonden.")
    return FileResponse(path=str(pdf_path), media_type="application/pdf", filename=filename)
