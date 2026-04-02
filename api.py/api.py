from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import json
import os

app = Flask(__name__)
CORS(app)

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CSV_PATH         = os.path.join(BASE_DIR, "..", "dataset", "student_dataset.csv")
OUTPUT_DIR       = os.path.join(BASE_DIR, "..", "output")
PROCESSED_PATH   = os.path.join(OUTPUT_DIR, "students_processed.json")
SUMMARY_PATH     = os.path.join(OUTPUT_DIR, "summary.json")

def load_data():
    """Load from Airflow output if available, else fall back to raw CSV."""
    if os.path.exists(PROCESSED_PATH):
        with open(PROCESSED_PATH, "r") as f:
            return json.load(f), "airflow_output"
    else:
        df = pd.read_csv(CSV_PATH)
        df.columns = df.columns.str.strip()
        df['Grade']     = df['Grade'].str.strip()
        df['Math']      = pd.to_numeric(df['Math'],      errors='coerce').fillna(0).astype(int)
        df['Physics']   = pd.to_numeric(df['Physics'],   errors='coerce').fillna(0).astype(int)
        df['Chemistry'] = pd.to_numeric(df['Chemistry'], errors='coerce').fillna(0).astype(int)
        df['Total']     = df['Math'] + df['Physics'] + df['Chemistry']
        return df.to_dict(orient="records"), "raw_csv"

@app.route("/data")
def get_data():
    data, source = load_data()
    return jsonify(data)

@app.route("/summary")
def get_summary():
    if os.path.exists(SUMMARY_PATH):
        with open(SUMMARY_PATH, "r") as f:
            return jsonify(json.load(f))
    # Build summary on the fly from CSV
    data, _ = load_data()
    n = len(data)
    gc = {}
    math = phy = chem = total_sum = top = fail = 0
    for s in data:
        g = (s.get('Grade') or '').strip()
        gc[g] = gc.get(g, 0) + 1
        m, p, c = int(s.get('Math',0)), int(s.get('Physics',0)), int(s.get('Chemistry',0))
        math += m; phy += p; chem += c
        tot = m + p + c
        total_sum += tot
        if tot > top: top = tot
        if g == 'F': fail += 1
    return jsonify({
        "last_updated"   : "Not yet run by Airflow",
        "total_students" : n,
        "grade_counts"   : gc,
        "math_avg"       : round(math/n, 1),
        "physics_avg"    : round(phy/n, 1),
        "chemistry_avg"  : round(chem/n, 1),
        "avg_total"      : round(total_sum/n, 1),
        "top_score"      : top,
        "fail_count"     : fail,
        "pass_rate"      : round(((n-fail)/n)*100, 1)
    })

@app.route("/status")
def status():
    airflow_ran = os.path.exists(PROCESSED_PATH)
    return jsonify({
        "api"           : "running",
        "data_source"   : "airflow_output" if airflow_ran else "raw_csv",
        "airflow_ran"   : airflow_ran,
        "processed_file": PROCESSED_PATH if airflow_ran else None
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)
