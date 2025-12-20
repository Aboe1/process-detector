# === PROLIXIA – PROCESS DETECTOR (Demo + PDF + HTML email) ===

from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import os
import json
import hmac
import hashlib
import subprocess
import sys
import shutil
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
import stripe

app = FastAPI(title="Prolixia – Process Detector")

# ========== PATHS ==========
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ========== CONFIG ==========
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me")
DEV_MODE = os.getenv("DEV_MODE") == "true"

# Stripe (optioneel: kan leeg zijn als je alleen demo test)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC")
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

TENANTS_FILE = DATA_DIR / "tenants.json"

# Demo
DEMO_CSV = UPLOAD_DIR / "demo.csv"
DEMO_RATE = 60

# SMTP (Strato)
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
MAIL_FROM = os.getenv("MAIL_FROM", SMTP_USER)


# ========== HELPERS ==========
def load_tenants() -> dict:
    if TENANTS_FILE.exists():
        try:
            return json.loads(TENANTS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def sign(email: str) -> str:
    sig = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return f"{email}.{sig}"

def verify(token: str | None) -> str | None:
    if not token or "." not in token:
        return None
    email, sig = token.rsplit(".", 1)
    check = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return email if hmac.compare_digest(sig, check) else None

def get_user(request: Request) -> str | None:
    return verify(request.cookies.get("pd_token"))

def is_active(email: str | None) -> bool:
    if not email:
        return False
    return bool(load_tenants().get(email, {}).get("active", False))


# ========== EMAIL ==========
def send_pdf_email(to_email: str, pdf_path: Path) -> None:
    # If SMTP not configured, do nothing
    if not all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM]):
        return

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"])
    )
    template = env.get_template("email_report.html")
    html_content = template.render(email=to_email)

    msg = EmailMessage()
    msg["Subject"] = "Je Prolixia rapport (Support analyse)"
    msg["From"] = MAIL_FROM
    msg["To"] = to_email

    # plain text fallback
    msg.set_content(
        "Je Prolixia rapport is gegenereerd.\n"
        "Bekijk de bijlage voor het PDF-rapport."
    )

    # HTML version
    msg.add_alternative(html_content, subtype="html")

    # Attach PDF
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
    except Exception as e:
        # For debugging in Render logs
        print("SMTP ERROR:", e)


# ========== ROUTES ==========
@app.get("/landing", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


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


@app.post("/demo")
async def demo(request: Request):
    # Allow unlimited demo in DEV_MODE, otherwise 1x per browser
    if not DEV_MODE and request.cookies.get("pd_demo_used") == "true":
        raise HTTPException(status_code=403, detail="Demo al gebruikt")

    if not DEMO_CSV.exists():
        raise HTTPException(status_code=500, detail="Demo CSV ontbreekt. Voeg uploads/demo.csv toe aan je repo.")

    shutil.copyfile(DEMO_CSV, UPLOAD_DIR / "events.csv")
    pdf_name = "process_report_demo.pdf"

    subprocess.run(
        [sys.executable, "analyze.py", str(DEMO_RATE), pdf_name],
        check=True
    )

    # Important: 303 forces GET after POST (prevents Method Not Allowed)
    resp = RedirectResponse(f"/download/{pdf_name}", status_code=303)
    resp.set_cookie("pd_demo_used", "true", max_age=60 * 60 * 24 * 365)
    return resp


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    rate: float = Form(...),
):
    email = get_user(request)
    if not email or not is_active(email):
        raise HTTPException(status_code=402, detail="Abonnement vereist")

    (UPLOAD_DIR / "events.csv").write_bytes(await file.read())

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    pdf_name = f"process_report_{stamp}.pdf"

    subprocess.run(
        [sys.executable, "analyze.py", str(rate), pdf_name],
        check=True
    )

    pdf_path = UPLOAD_DIR / pdf_name

    # Send email (non-blocking errors)
    send_pdf_email(email, pdf_path)

    return JSONResponse({"filename": pdf_name})


@app.get("/download/{filename}")
def download(filename: str):
    path = UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Bestand niet gevonden")
    return FileResponse(path, media_type="application/pdf", filename=filename)
