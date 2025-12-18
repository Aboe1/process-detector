# === PROCESS DETECTOR – PRO COMPARE FEATURE ===

from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import os, json, hmac, hashlib, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import stripe

app = FastAPI(title="Process Detector")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ========== CONFIG ==========
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC")
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me")

BASIC_LIMIT = 5
PRO_LIMIT = 999999

stripe.api_key = STRIPE_SECRET_KEY
TENANTS_FILE = DATA_DIR / "tenants.json"

# ========== HELPERS ==========
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

def get_user(request):
    return verify(request.cookies.get("pd_token"))

def is_active(email):
    return load_tenants().get(email, {}).get("active", False)

def current_month():
    now = datetime.now(timezone.utc)
    return f"{now.year}-{now.month:02d}"

# ========== ROUTES ==========
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    if email and user.get("usage_month") != current_month():
        user["usage_month"] = current_month()
        user["uploads"] = 0
        tenants[email] = user
        save_tenants(tenants)

    plan = user.get("plan", "basic")

    return templates.TemplateResponse("index.html", {
        "request": request,
        "email": email,
        "active": is_active(email) if email else False,
        "plan": plan,
        "used": user.get("uploads", 0),
        "limit": PRO_LIMIT if plan == "pro" else BASIC_LIMIT,
        "is_pro": plan == "pro",
        "history": user.get("history", []) if plan == "pro" else []
    })

# ========== COMPARE (PRO ONLY) ==========
@app.post("/compare")
async def compare(request: Request):
    email = get_user(request)
    if not email:
        raise HTTPException(status_code=401)

    tenants = load_tenants()
    user = tenants.get(email)
    if not user or user.get("plan") != "pro":
        raise HTTPException(status_code=403, detail="Alleen beschikbaar voor Pro.")

    form = await request.form()
    a = int(form.get("a", -1))
    b = int(form.get("b", -1))

    history = user.get("history", [])
    if a < 0 or b < 0 or a == b or a >= len(history) or b >= len(history):
        raise HTTPException(status_code=400, detail="Selecteer twee geldige analyses.")

    h1 = history[a]
    h2 = history[b]

    delta_hours = h2.get("impact_hours", 0) - h1.get("impact_hours", 0)
    delta_eur = h2.get("impact_eur", 0) - h1.get("impact_eur", 0)

    return JSONResponse({
        "from": h1["date"],
        "to": h2["date"],
        "delta_hours": round(delta_hours, 2),
        "delta_eur": round(delta_eur, 2),
        "improved": delta_hours < 0
    })
