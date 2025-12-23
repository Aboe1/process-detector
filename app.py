from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime, timezone
import os, json, stripe, subprocess, sys, shutil, hmac, hashlib

# ================= SETUP =================
app = FastAPI(title="Prolixia – Support Process Analyzer")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"

UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

TENANTS_FILE = DATA_DIR / "tenants.json"
DEMO_CSV = UPLOAD_DIR / "demo.csv"
LAST_METRICS = UPLOAD_DIR / "last_metrics.json"

# ================= CONFIG =================
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_BASIC = os.getenv("STRIPE_PRICE_BASIC")
STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
TOKEN_SIGNING_SECRET = os.getenv("TOKEN_SIGNING_SECRET", "change-me")

stripe.api_key = STRIPE_SECRET_KEY

# ================= HELPERS =================
def load_tenants():
    if TENANTS_FILE.exists():
        try:
            return json.loads(TENANTS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_tenants(data):
    TENANTS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

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
        try:
            return json.loads(LAST_METRICS.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


# ================= ROUTES =================
@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/app", response_class=HTMLResponse)
def app_home(request: Request):
    email = get_user(request)
    tenants = load_tenants()
    user = tenants.get(email, {}) if email else {}

    demo_used = request.cookies.get("pd_demo_used") == "true"
    last_demo_pdf = request.cookies.get("pd_last_demo_pdf")
    metrics = read_last_metrics()

    # ROI values to show on page (optional)
    roi_month_eur = None
    roi_year_eur = None
    if metrics:
        roi_month_eur = metrics.get("monthly_eur_est") or metrics.get("monthly_eur") or None
        roi_year_eur = metrics.get("yearly_eur_est") or metrics.get("yearly_eur") or None

    return templates.TemplateResponse("index.html", {
    "request": request,
    "email": email,
    "active": is_active(email),
    "plan": user.get("plan", "basic"),
    "demo_used": demo_used,
    "last_demo_pdf": last_demo_pdf,
    "roi_month_eur": roi_month_eur,
    "roi_year_eur": roi_year_eur,
    "metrics": metrics,  
})

    })


# ================= DEMO =================
@app.post("/demo")
def demo(request: Request):
    # 1 demo per browser
    if request.cookies.get("pd_demo_used") == "true":
        raise HTTPException(status_code=403, detail="Demo al gebruikt")

    if not DEMO_CSV.exists():
        raise HTTPException(status_code=500, detail="Demo CSV ontbreekt. Voeg uploads/demo.csv toe aan je repo.")

    shutil.copyfile(DEMO_CSV, UPLOAD_DIR / "events.csv")

    pdf_name = "process_report_demo.pdf"
    subprocess.run([sys.executable, "analyze.py", "60", pdf_name], check=True)

    resp = RedirectResponse(url=f"/download/{pdf_name}", status_code=303)
    resp.set_cookie("pd_demo_used", "true", max_age=31536000)
    resp.set_cookie("pd_last_demo_pdf", pdf_name, max_age=31536000)
    return resp


# ================= STRIPE CHECKOUT =================
@app.post("/subscribe/{plan}")
def subscribe(plan: str, email: str = Form(...)):
    if plan not in ("basic", "pro"):
        raise HTTPException(status_code=400, detail="Ongeldig plan")

    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_BASIC or not STRIPE_PRICE_PRO:
        raise HTTPException(status_code=500, detail="Stripe is niet geconfigureerd")

    price_id = STRIPE_PRICE_BASIC if plan == "basic" else STRIPE_PRICE_PRO

    session = stripe.checkout.Session.create(
        mode="subscription",
        payment_method_types=["card", "ideal"],
        line_items=[{"price": price_id, "quantity": 1}],
        customer_email=email,
        success_url=f"{BASE_URL}/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/app",
        metadata={"plan": plan, "email": email},
    )

    return RedirectResponse(session.url, status_code=303)


# ================= STRIPE SUCCESS =================
@app.get("/success")
def success(session_id: str):
    session = stripe.checkout.Session.retrieve(session_id)

    email = session.customer_email
    plan = session.metadata.get("plan", "basic") if session.metadata else "basic"

    tenants = load_tenants()
    tenants[email] = {
        "active": True,
        "plan": plan,
        "since": datetime.now(timezone.utc).isoformat()
    }
    save_tenants(tenants)

    resp = RedirectResponse("/app", status_code=303)
    resp.set_cookie("pd_token", sign(email), httponly=True, max_age=31536000)
    return resp


# ================= WEBHOOK =================
@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook")

    if event["type"] == "customer.subscription.deleted":
        sub = event["data"]["object"]
        email = sub.get("customer_email")
        if email:
            tenants = load_tenants()
            if email in tenants:
                tenants[email]["active"] = False
                save_tenants(tenants)

    return JSONResponse({"status": "ok"})


# ================= UPLOAD (PAID ONLY) =================
@app.post("/upload")
async def upload(request: Request, file: UploadFile = File(...), rate: int = Form(...)):
    email = get_user(request)
    if not email or not is_active(email):
        raise HTTPException(status_code=402, detail="Abonnement vereist")

    (UPLOAD_DIR / "events.csv").write_bytes(await file.read())

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    pdf_name = f"process_report_{stamp}.pdf"

    subprocess.run([sys.executable, "analyze.py", str(rate), pdf_name], check=True)

    return {"filename": pdf_name}


# ================= DOWNLOAD =================
@app.get("/download/{filename}")
def download(filename: str):
    path = UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Bestand niet gevonden")
    return FileResponse(path, media_type="application/pdf")
