import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

EMAIL = 'kgsou1605@gmail.com'
PASS  = 'zczxzgtonsdmbint'

try:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Airflow Student ETL - Test Notification'
    msg['From']    = EMAIL
    msg['To']      = EMAIL
    msg.attach(MIMEText('Airflow SMTP test email from Student ETL project.', 'plain'))
    msg.attach(MIMEText('<h2 style="color:#7c3aed">Airflow SMTP Working!</h2><p>Student ETL pipeline email is configured correctly.</p>', 'html'))

    s = smtplib.SMTP('smtp.gmail.com', 587, timeout=20)
    s.ehlo()
    s.starttls()
    s.login(EMAIL, PASS)
    s.send_message(msg)
    s.quit()
    print('SUCCESS - Email sent to', EMAIL)
except Exception as e:
    print('FAILED:', str(e))
