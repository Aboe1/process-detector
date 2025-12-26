from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime, timezone
import os, json, stripe, subprocess, sys, shutil, hmac, hashlib

app = FastAPI(title="Prolixia – Support SLA Intelligence")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

TENANTS_FILE = DATA_DIR / "tenants.json"
LAST_METRICS = UPLOAD_DIR / "last_metrics.json"
DEMO_CSV = UPLOAD_DIR / "demo.csv"

# ================= CONFIG =================
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC")
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")
STRIPE_PRICE_ENTERPRISE = os.getenv("STRIPE_PRICE_ENTERPRISE")
BASE_URL = os.getenv("BASE_URL", "https://www.prolixia.com")
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me")

stripe.api_key = STRIPE_SECRET_KEY

# ================= HELPERS =================
def load_tenants():
    if TENANTS_FILE.exists():
        return json.loads(TENANTS_FILE.read_text(encoding="utf-8"))
    return {}

def save_tenants(data):
    TENANTS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

def sign(email: str):
    sig = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return f"{email}.{sig}"

def verify(token: str | None):
    if not token or "." not in token:
        return None
    email, sig = token.rsplit(".", 1)
    check = hmac.new(TOKEN_SIGNING_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    return email if hmac.compare_digest(sig, check) else None

def get_user(request: Request):
    return verify(request.cookies.get("pd_token"))

def is_active(email: str | None):
    if not email:
        return False
    return load_tenants().get(email, {}).get("active", False)

def read_last_metrics():
    if LAST_METRICS.exists():
        return json.loads(LAST_METRICS.read_text(encoding="utf-8"))
    return {}

# ================= ROUTES =================
@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})

@app.get("/enterprise", response_class=HTMLResponse)
def enterprise(request: Request):
    return templates.TemplateResponse("enterprise.html", {"request": request})

@app.get("/app", response_class=HTMLResponse)
def app_home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    metrics = read_last_metrics() or {}

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "email": email,
            "active": is_active(email),
            "plan": user.get("plan", "basic"),
            "demo_used": request.cookies.get("pd_demo_used") == "true",
            "last_demo_pdf": request.cookies.get("pd_last_demo_pdf"),
            "roi_month_eur": metrics.get("impact", {}).get("monthly_eur_est"),
            "metrics": metrics,
        },
    )

# ================= DEMO =================
@app.get("/demo")
def demo_get():
    """
    Landingpage demo-knop → start demo en ga naar /app
    """
    shutil.copyfile(DEMO_CSV, UPLOAD_DIR / "events.csv")
    pdf_name = "process_report_demo.pdf"

    subprocess.run(
        [sys.executable, "analyze.py", "60", pdf_name, "demo"],
        check=True
    )

    resp = RedirectResponse(url="/app", status_code=303)
    resp.set_cookie("pd_demo_used", "true", max_age=31536000)
    resp.set_cookie("pd_last_demo_pdf", pdf_name, max_age=31536000)
    return resp

@app.post("/demo")
def demo_post():
    return demo_get()

# ================= STRIPE =================
@app.post("/subscribe/{plan}")
def subscribe(plan: str, email: str = Form(...)):
    if plan not in ("basic", "pro", "enterprise"):
        raise HTTPException(400)

    price_id = {
        "basic": STRIPE_PRICE_BASIC,
        "pro": STRIPE_PRICE_PRO,
        "enterprise": STRIPE_PRICE_ENTERPRISE
    }[plan]

    session = stripe.checkout.Session.create(
        mode="subscription",
        payment_method_types=["card", "ideal"],
        line_items=[{"price": price_id, "quantity": 1}],
        customer_email=email,
        success_url=f"{BASE_URL}/app",
        cancel_url=f"{BASE_URL}/app",
        metadata={"plan": plan, "email": email},
    )
    return RedirectResponse(session.url, status_code=303)

# ================= UPLOAD =================
@app.post("/upload")
async def upload(request: Request, file: UploadFile = File(...), rate: int = Form(...)):
    email = get_user(request)
    if not email or not is_active(email):
        raise HTTPException(402)

    (UPLOAD_DIR / "events.csv").write_bytes(await file.read())

    pdf_name = f"process_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.pdf"
    subprocess.run(
        [sys.executable, "analyze.py", str(rate), pdf_name, email],
        check=True
    )
    return {"filename": pdf_name}

@app.get("/download/{filename}")
def download(filename: str):
    return FileResponse(UPLOAD_DIR / filename, media_type="application/pdf")

