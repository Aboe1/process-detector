# === PROCESS DETECTOR – DEMO MODE ===

from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.responses import Response

import os, json, hmac, hashlib, subprocess, sys, shutil
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

DEMO_CSV = UPLOAD_DIR / "demo.csv"
DEMO_PDF = UPLOAD_DIR / "process_report_demo.pdf"
DEMO_RATE = 60  # vaste € / uur

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

# ========== ROUTES ==========
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    plan = user.get("plan", "basic")

    return templates.TemplateResponse("index.html", {
        "request": request,
        "email": email,
        "active": is_active(email) if email else False,
        "plan": plan,
        "is_pro": plan == "pro",
        "demo_used": request.cookies.get("pd_demo_used") == "true"
    })

# ========== DEMO MODE ==========
@app.post("/demo")
async def demo(request: Request):
    if request.cookies.get("pd_demo_used") == "true":
        raise HTTPException(status_code=403, detail="Demo al gebruikt.")

    if not DEMO_CSV.exists():
        raise HTTPException(status_code=500, detail="Demo CSV ontbreekt.")

    # kopieer demo.csv → events.csv
    shutil.copyfile(DEMO_CSV, UPLOAD_DIR / "events.csv")

    # run analyse
    subprocess.run(
        [sys.executable, "analyze.py", str(DEMO_RATE), DEMO_PDF.name],
        check=True
    )

    resp = RedirectResponse(f"/download/{DEMO_PDF.name}")
    resp.set_cookie("pd_demo_used", "true", max_age=60*60*24*365)
    return resp

# ========== DOWNLOAD ==========
@app.get("/download/{filename}")
def download(filename: str):
    path = UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(path, media_type="application/pdf")
