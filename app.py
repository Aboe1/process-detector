from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import os, time, glob, json, hmac, hashlib, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
import stripe

app = FastAPI(title="Process Detector")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ================= CONFIG =================
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC")
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me")

BASIC_LIMIT = 5
PRO_LIMIT = 999999

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

TENANTS_FILE = DATA_DIR / "tenants.json"


# ================= HELPERS =================
def load_tenants():
    if TENANTS_FILE.exists():
        return json.loads(TENANTS_FILE.read_text())
    return {}

def save_tenants(data):
    TENANTS_FILE.write_text(json.dumps(data, indent=2))

def sign(email):
    sig = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return f"{email}.{sig}"

def verify(token):
    if not token or "." not in token:
        return None
    email, sig = token.rsplit(".", 1)
    check = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return email if hmac.compare_digest(sig, check) else None

def current_month():
    now = datetime.now(timezone.utc)
    return f"{now.year}-{now.month:02d}"

def get_user(request):
    return verify(request.cookies.get("pd_token"))

def is_active(email):
    return load_tenants().get(email, {}).get("active", False)


# ================= ROUTES =================
@app.get("/landing", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
def app_home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    if user.get("usage_month") != current_month():
        user["usage_month"] = current_month()
        user["uploads"] = 0
        tenants[email] = user
        save_tenants(tenants)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "email": email,
        "active": is_active(email) if email else False,
        "plan": user.get("plan"),
        "used": user.get("uploads", 0),
        "limit": PRO_LIMIT if user.get("plan") == "pro" else BASIC_LIMIT
    })


# ================= STRIPE =================
@app.post("/billing/checkout")
async def checkout(request: Request):
    form = await request.form()
    email = form.get("email")
    plan = form.get("plan", "basic")

    price = STRIPE_PRICE_PRO if plan == "pro" else STRIPE_PRICE_BASIC

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price, "quantity": 1}],
        success_url=f"{BASE_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/billing/cancel",
        customer_email=email,
        metadata={"email": email, "plan": plan},
    )
    return RedirectResponse(session.url, status_code=303)


@app.get("/billing/success", response_class=HTMLResponse)
def success(request: Request, session_id: str):
    sess = stripe.checkout.Session.retrieve(session_id)
    email = sess.customer_email

    tenants = load_tenants()
    tenants[email] = {
        "email": email,
        "plan": sess.metadata.get("plan"),
        "customer_id": sess.customer,
        "subscription_id": sess.subscription,
        "active": True,
        "usage_month": current_month(),
        "uploads": 0
    }
    save_tenants(tenants)

    resp = templates.TemplateResponse("billing_result.html", {
        "request": request,
        "ok": True,
        "email": email
    })
    resp.set_cookie("pd_token", sign(email), httponly=True)
    return resp


@app.get("/billing/cancel", response_class=HTMLResponse)
def cancel(request: Request):
    return templates.TemplateResponse("billing_result.html", {
        "request": request,
        "ok": False
    })


@app.post("/stripe/webhook")
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature")
    event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)

    tenants = load_tenants()

    if event["type"].startswith("customer.subscription"):
        sub = event["data"]["object"]
        for email, data in tenants.items():
            if data.get("customer_id") == sub.customer:
                data["active"] = sub.status in ("active", "trialing")
                tenants[email] = data
                save_tenants(tenants)
                break

    return {"ok": True}


# ================= CORE APP =================
@app.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    email = get_user(request)
    if not email or not is_active(email):
        raise HTTPException(status_code=402, detail="Abonnement vereist")

    tenants = load_tenants()
    user = tenants[email]

    limit = PRO_LIMIT if user["plan"] == "pro" else BASIC_LIMIT
    if user["uploads"] >= limit:
        raise HTTPException(status_code=403, detail="Uploadlimiet bereikt")

    path = UPLOAD_DIR / "events.csv"
    path.write_bytes(await file.read())

    subprocess.run([sys.executable, "analyze.py"], check=True)

    user["uploads"] += 1
    tenants[email] = user
    save_tenants(tenants)

    return {"filename": "process_report.pdf"}


@app.get("/download/{filename}")
def download(filename: str):
    return FileResponse(UPLOAD_DIR / filename, media_type="application/pdf")
