# === PROCESS DETECTOR – AUTO EMAIL PDF ===

from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import os, json, hmac, hashlib, subprocess, sys, shutil, smtplib
from email.message import EmailMessage
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

# 📧 SMTP CONFIG (Render env vars)
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)

BASIC_LIMIT = 5
PRO_LIMIT = 999999

stripe.api_key = STRIPE_SECRET_KEY
TENANTS_FILE = DATA_DIR / "tenants.json"

DEMO_CSV = UPLOAD_DIR / "demo.csv"
DEMO_RATE = 60

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

# ========== EMAIL ==========
def send_pdf_email(to_email: str, pdf_path: Path):
    if not all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM]):
        return  # mail niet geconfigureerd → stil falen

    msg = EmailMessage()
    msg["Subject"] = "Je Process Detector rapport"
    msg["From"] = MAIL_FROM
    msg["To"] = to_email

    msg.set_content(
        "Beste,\n\n"
        "In de bijlage vind je het gegenereerde Process Detector rapport.\n\n"
        "Met vriendelijke groet,\n"
        "Process Detector"
    )

    with open(pdf_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename=pdf_path.name
        )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    except Exception:
        pass  # mailfout mag de app niet breken

# ========== ROUTES ==========
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    return templates.TemplateResponse("index.html", {
        "request": request,
        "email": email,
        "active": is_active(email) if email else False,
        "plan": user.get("plan", "basic"),
        "demo_used": request.cookies.get("pd_demo_used") == "true"
    })

# ========== DEMO ==========
@app.post("/demo")
async def demo(request: Request):
    if request.cookies.get("pd_demo_used") == "true":
        raise HTTPException(status_code=403, detail="Demo al gebruikt")

    shutil.copyfile(DEMO_CSV, UPLOAD_DIR / "events.csv")
    pdf_name = "process_report_demo.pdf"

    subprocess.run(
        [sys.executable, "analyze.py", str(DEMO_RATE), pdf_name],
        check=True
    )

    resp = RedirectResponse(
    f"/download/{pdf_name}",
    status_code=303
)
    resp.set_cookie("pd_demo_used", "true", max_age=60*60*24*365)
    return resp

# ========== UPLOAD ==========
@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    rate: float = Form(...),
):
    email = get_user(request)
    if not email or not is_active(email):
        raise HTTPException(status_code=402)

    (UPLOAD_DIR / "events.csv").write_bytes(await file.read())

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    pdf_name = f"process_report_{stamp}.pdf"

    subprocess.run(
        [sys.executable, "analyze.py", str(rate), pdf_name],
        check=True
    )

    pdf_path = UPLOAD_DIR / pdf_name

    # 📧 stuur mail
    send_pdf_email(email, pdf_path)

    return {"filename": pdf_name}

# ========== DOWNLOAD ==========
@app.get("/download/{filename}")
def download(filename: str):
    path = UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(path, media_type="application/pdf")

