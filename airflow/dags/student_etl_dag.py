from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import json
import os

# ── Config ────────────────────────────────────────────────────
CSV_PATH     = "/opt/airflow/dags/student_dataset.csv"
OUTPUT_PATH  = "/opt/airflow/dags/students_processed.json"
SUMMARY_PATH = "/opt/airflow/dags/summary.json"
NOTIFY_EMAIL = "kgsou1605@gmail.com"
SMTP_HOST    = "smtp.gmail.com"
SMTP_PORT    = 587
SMTP_USER    = "kgsou1605@gmail.com"
SMTP_PASS    = "zczxzgtonsdmbint"

# ── Email callbacks ───────────────────────────────────────────
def _send_mail(subject, html):
    """Send email directly via smtplib — bypasses Airflow SMTP config issues."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SMTP_USER
    msg['To']      = NOTIFY_EMAIL
    msg.attach(MIMEText(html, 'html'))
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20)
    s.ehlo()
    s.starttls()
    s.login(SMTP_USER, SMTP_PASS)
    s.send_message(msg)
    s.quit()
    print(f'Email sent to {NOTIFY_EMAIL}')


def on_failure_email(context):
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exc     = str(context.get('exception', 'Unknown error'))[:300]
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
      <div style="background:#ef4444;padding:20px;border-radius:8px 8px 0 0">
        <h2 style="color:white;margin:0">❌ Airflow Pipeline FAILED</h2>
      </div>
      <div style="background:#1e1e2e;padding:24px;border-radius:0 0 8px 8px;color:#e2e8f0">
        <table style="width:100%;border-collapse:collapse">
          <tr><td style="padding:8px;color:#94a3b8">DAG</td><td style="padding:8px;font-weight:bold">{dag_id}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Failed Task</td><td style="padding:8px;color:#f87171;font-weight:bold">{task_id}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Time</td><td style="padding:8px">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Error</td><td style="padding:8px;color:#fca5a5">{exc}</td></tr>
        </table>
      </div>
    </div>"""
    _send_mail(f"❌ Airflow FAILED: {dag_id} → {task_id}", html)


def on_retry_email(context):
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    attempt = context['task_instance'].try_number
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
      <div style="background:#f59e0b;padding:20px;border-radius:8px 8px 0 0">
        <h2 style="color:white;margin:0">⚠️ Airflow Task RETRYING</h2>
      </div>
      <div style="background:#1e1e2e;padding:24px;border-radius:0 0 8px 8px;color:#e2e8f0">
        <p>Task <strong>{task_id}</strong> in DAG <strong>{dag_id}</strong> retrying — attempt <strong>{attempt}</strong>.</p>
        <p style="color:#94a3b8;margin-top:8px">Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
      </div>
    </div>"""
    _send_mail(f"⚠️ Airflow RETRY: {dag_id} → {task_id} (attempt {attempt})", html)


# ── ETL function ──────────────────────────────────────────────
def run_etl():
    os.makedirs("/opt/airflow/dags", exist_ok=True)

    df = pd.read_csv(CSV_PATH)
    df.columns   = df.columns.str.strip()
    df['Math']      = pd.to_numeric(df['Math'],      errors='coerce').fillna(0).astype(int)
    df['Physics']   = pd.to_numeric(df['Physics'],   errors='coerce').fillna(0).astype(int)
    df['Chemistry'] = pd.to_numeric(df['Chemistry'], errors='coerce').fillna(0).astype(int)
    df['Grade']     = df['Grade'].str.strip()
    df['Total']     = df['Math'] + df['Physics'] + df['Chemistry']

    df.to_json(OUTPUT_PATH, orient='records', indent=2)

    n            = len(df)
    grade_counts = df['Grade'].value_counts().to_dict()
    fail_count   = grade_counts.get('F', 0)

    summary = {
        "last_updated"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_students": n,
        "grade_counts"  : grade_counts,
        "math_avg"      : round(df['Math'].mean(), 1),
        "physics_avg"   : round(df['Physics'].mean(), 1),
        "chemistry_avg" : round(df['Chemistry'].mean(), 1),
        "avg_total"     : round(df['Total'].mean(), 1),
        "top_score"     : int(df['Total'].max()),
        "fail_count"    : fail_count,
        "pass_rate"     : round(((n - fail_count) / n) * 100, 1)
    }

    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)

    print("=== ETL Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    return summary


def send_success_email(**context):
    # Read summary directly from file — avoids XCom serialization issues
    with open(SUMMARY_PATH, 'r') as f:
        summary = json.load(f)
    gc = summary.get('grade_counts', {})

    grade_rows = "".join([
        f"<tr><td style='padding:6px 12px;color:#94a3b8'>{g}</td>"
        f"<td style='padding:6px 12px;font-weight:bold'>{gc.get(g, 0)}</td></tr>"
        for g in ['A+', 'A', 'B+', 'B', 'C', 'D', 'F']
    ])

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
      <div style="background:linear-gradient(135deg,#7c3aed,#06b6d4);
                  padding:24px;border-radius:8px 8px 0 0">
        <h2 style="color:white;margin:0">✅ Student ETL Pipeline Completed</h2>
        <p style="color:rgba(255,255,255,0.8);margin:6px 0 0">
          {summary.get('last_updated', '')}
        </p>
      </div>
      <div style="background:#1e1e2e;padding:24px;border-radius:0 0 8px 8px;color:#e2e8f0">

        <h3 style="color:#a78bfa;margin:0 0 16px">📊 Summary</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:20px">
          <tr><td style="padding:8px;color:#94a3b8">Total Students</td>
              <td style="padding:8px;font-weight:bold;font-size:18px">{summary.get('total_students')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Pass Rate</td>
              <td style="padding:8px;color:#34d399;font-weight:bold">{summary.get('pass_rate')}%</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Failed Students</td>
              <td style="padding:8px;color:#f87171;font-weight:bold">{summary.get('fail_count')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Avg Total Score</td>
              <td style="padding:8px;font-weight:bold">{summary.get('avg_total')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Top Score</td>
              <td style="padding:8px;font-weight:bold">{summary.get('top_score')}</td></tr>
        </table>

        <h3 style="color:#a78bfa;margin:0 0 12px">📈 Subject Averages</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:20px">
          <tr><td style="padding:8px;color:#94a3b8">Math</td>
              <td style="padding:8px;font-weight:bold">{summary.get('math_avg')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Physics</td>
              <td style="padding:8px;font-weight:bold">{summary.get('physics_avg')}</td></tr>
          <tr><td style="padding:8px;color:#94a3b8">Chemistry</td>
              <td style="padding:8px;font-weight:bold">{summary.get('chemistry_avg')}</td></tr>
        </table>

        <h3 style="color:#a78bfa;margin:0 0 12px">🎓 Grade Breakdown</h3>
        <table style="width:100%;border-collapse:collapse">
          {grade_rows}
        </table>

        <p style="margin-top:24px;color:#64748b;font-size:12px">
          Automated notification from Student ETL Airflow Pipeline.
        </p>
      </div>
    </div>
    """
    _send_mail(
        subject=f"✅ Student ETL Done — {summary.get('total_students')} students | Pass Rate: {summary.get('pass_rate')}%",
        html=html
    )


# ── DAG ───────────────────────────────────────────────────────
default_args = {
    'owner'             : 'airflow',
    'retries'           : 1,
    'retry_delay'       : timedelta(minutes=5),
    'on_failure_callback': on_failure_email,
    'on_retry_callback' : on_retry_email,
}

with DAG(
    dag_id="student_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["student", "etl"]
) as dag:

    check_csv = BashOperator(
        task_id="check_csv_exists",
        bash_command=f"test -f {CSV_PATH} && echo 'CSV OK' || (echo 'CSV NOT FOUND' && exit 1)"
    )

    etl_task = PythonOperator(
        task_id="run_etl_analysis",
        python_callable=run_etl
    )

    success_email = PythonOperator(
        task_id="send_success_email",
        python_callable=send_success_email
    )

    check_csv >> etl_task >> success_email
