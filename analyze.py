import sys
import os
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

# Zorg dat Windows console geen Unicode-issues geeft
sys.stdout.reconfigure(encoding="utf-8")

print("Process detector gestart")

try:
    # ==================================================
    # INPUT
    # ==================================================
    INPUT_FILE = "uploads/events.csv"

    if not os.path.exists(INPUT_FILE):
        print("[WARN] Geen CSV gevonden in uploads/events.csv")
        sys.exit(0)

    df = pd.read_csv(INPUT_FILE)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["case_id", "timestamp"])

    # ==================================================
    # DUUR PER STAP
    # ==================================================
    df["next_time"] = df.groupby("case_id")["timestamp"].shift(-1)
    df["step_duration"] = (
        df["next_time"] - df["timestamp"]
    ).dt.total_seconds() / 3600

    # ==================================================
    # PROBLEMEN DETECTEREN
    # ==================================================
    problems = []

    for step, group in df.groupby("step"):
        durations = group["step_duration"].dropna()
        if len(durations) < 2:
            continue

        normal = durations.mean()
        threshold = normal * 1.5

        for _, row in group.iterrows():
            actual = row["step_duration"]
            if pd.notna(actual) and actual > threshold:
                problems.append({
                    "case_id": int(row["case_id"]),
                    "step": step,
                    "delay": actual - normal,
                    "factor": actual / normal,
                    "actual": actual,
                    "normal": normal
                })

    problems_sorted = sorted(
        problems, key=lambda p: p["delay"], reverse=True
    )

    # ==================================================
    # STRUCTURELE KNELPUNTEN PER STAP
    # ==================================================
    step_summary = {}

    for p in problems:
        step_summary.setdefault(
            p["step"], {"total_delay": 0.0, "count": 0}
        )
        step_summary[p["step"]]["total_delay"] += p["delay"]
        step_summary[p["step"]]["count"] += 1

    sorted_steps = sorted(
        step_summary.items(),
        key=lambda x: x[1]["total_delay"],
        reverse=True
    )

    # ==================================================
    # PDF RAPPORT GENEREREN
    # ==================================================
    doc = SimpleDocTemplate("process-report.pdf")
    styles = getSampleStyleSheet()
    elements = []

    elements.append(
        Paragraph("<b>PROCESS ANALYSE RAPPORT</b>", styles["Title"])
    )
    elements.append(Spacer(1, 12))

    elements.append(
        Paragraph("<b>Top procesproblemen</b>", styles["Heading2"])
    )
    elements.append(Spacer(1, 8))

    if not problems_sorted:
        elements.append(
            Paragraph("Geen procesproblemen gevonden.", styles["Normal"])
        )
    else:
        for p in problems_sorted:
            elements.append(Paragraph(
                f"<b>Case {p['case_id']} – stap '{p['step']}'</b><br/>"
                f"Impact: {p['delay']:.1f} uur vertraging<br/>"
                f"Oorzaak: {p['factor']:.1f}x langer dan normaal "
                f"({p['actual']:.1f}u vs {p['normal']:.1f}u)",
                styles["Normal"]
            ))
            elements.append(Spacer(1, 10))

    elements.append(Spacer(1, 16))
    elements.append(
        Paragraph("<b>Structurele knelpunten</b>", styles["Heading2"])
    )
    elements.append(Spacer(1, 8))

    if not sorted_steps:
        elements.append(
            Paragraph("Geen structurele knelpunten.", styles["Normal"])
        )
    else:
        for step, stats in sorted_steps:
            elements.append(Paragraph(
                f"<b>Stap '{step}'</b><br/>"
                f"Totale impact: {stats['total_delay']:.1f} uur<br/>"
                f"Aantal incidenten: {stats['count']}",
                styles["Normal"]
            ))
            elements.append(Spacer(1, 10))

    doc.build(elements)

    print("[OK] Analyse succesvol afgerond")

except Exception as e:
    # NOOIT emoji's hier, alleen ASCII
    print("[ERROR] Analyse fout:", str(e))
    sys.exit(0)
