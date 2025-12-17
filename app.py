from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import shutil
import os
import subprocess

app = FastAPI()

UPLOAD_DIR = "uploads"
REPORT_FILE = "process-report.pdf"

os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.get("/", response_class=HTMLResponse)
def index():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.post("/upload")
def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return JSONResponse(
            status_code=400,
            content={"error": "Upload een geldig CSV-bestand"}
        )

    csv_path = os.path.join(UPLOAD_DIR, "events.csv")

    # Opslaan
    with open(csv_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Analyse draaien (NOOIT crashen)
    result = subprocess.run(
        ["python", "analyze.py"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("❌ Analyse fout")
        print(result.stdout)
        print(result.stderr)
        return JSONResponse(
            status_code=500,
            content={
                "error": "Analyse mislukt",
                "details": result.stderr
            }
        )

    return JSONResponse(
        content={
            "status": "ok",
            "download": "/download"
        }
    )


@app.get("/download")
def download_report():
    if not os.path.exists(REPORT_FILE):
        return JSONResponse(
            status_code=404,
            content={"error": "Rapport nog niet beschikbaar"}
        )

    return FileResponse(
        REPORT_FILE,
        media_type="application/pdf",
        filename="process-report.pdf"
    )
