# 🎓 Student ETL Performance Analysis

A full-stack data engineering project that processes student performance data using an ETL pipeline with Apache Airflow, visualizes it through a beautiful interactive dashboard.

---

## 🚀 Features

### Frontend Dashboard
- 📊 **Overview** — 8 grade stat cards with animated counters
- 📈 **Analytics** — 6 interactive charts (Grade distribution, Score ranges, Subject averages)
- 🎓 **Student Records** — Searchable, sortable table with grade filters
- 👤 **Student Detail Modal** — Click any row to view full student profile
- 🌙 **Dark / Light Mode** — Theme toggle saved in localStorage
- 🏆 **Top 10 Leaderboard** — Ranked by total score with medals

### ETL Pipeline (Apache Airflow)
- ✅ CSV validation task
- 📊 Data processing & analytics computation
- ✉️ Email notification on success/failure/retry
- 📅 Scheduled daily runs

### Tech Stack
| Layer | Technology |
|---|---|
| Frontend | HTML, CSS, JavaScript |
| ETL Orchestration | Apache Airflow |
| Data Processing | Python, Pandas |
| Message Queue | Apache Kafka |
| Backend API | Flask |
| Database | PostgreSQL |
| Containerization | Docker |

---

## 📁 Project Structure

```
student-etl-project/
├── frontend/           # Dashboard (HTML/CSS/JS)
│   ├── index.html      # Dashboard with stats & leaderboard
│   ├── students.html   # Student records table
│   ├── analytics.html  # Charts & analytics
│   └── style.css       # Styles
├── airflow/
│   └── dags/
│       └── student_etl_dag.py   # Airflow DAG
├── api.py/
│   └── api.py          # Flask REST API
├── spark_jobs/
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   └── student_analysis.py
├── dataset/
│   └── student_dataset.csv     # 9000 student records
├── docker-compose.yml
└── requirements.txt
```

---

## ⚙️ Setup & Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate frontend data
```bash
python -c "import pandas as pd; df=pd.read_csv('dataset/student_dataset.csv'); df.columns=df.columns.str.strip(); open('frontend/data.js','w').write('var STUDENT_DATA='+df.to_json(orient='records')+';')"
```

### 3. Open Frontend
Open `frontend/index.html` in browser or use VS Code Live Server.

### 4. Run Flask API
```bash
cd api.py
python api.py
```

### 5. Start Airflow (Docker)
```bash
docker-compose up -d
```
Open → http://localhost:8080 | Username: `airflow` | Password: `admin123`

---

## 📊 Dataset

- **9000 students** from Martin Luther School
- Fields: Name, Roll No., Math, Physics, Chemistry, Grade, Comment, Phone, Address
- Grades: A+, A, B+, B, C, D, F

---

## 📧 Email Notifications

Airflow sends HTML email reports to configured email on:
- ✅ Pipeline success — full ETL summary with grade breakdown
- ❌ Task failure — error details
- ⚠️ Task retry — attempt number

---

## 👨‍💻 Author

**Soumya KG** — Infosys Springboard Project
