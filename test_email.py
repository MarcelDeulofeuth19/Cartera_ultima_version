import smtplib
from email.mime.text import MIMEText

msg = MIMEText('Test email from system.')
msg['Subject'] = 'Test Panel AloCredit'
msg['From'] = 'noreply@alocredit.co'
msg['To'] = 'mdeulofeuth@alocredit.co'

try:
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.ehlo('alocredit.co')
        server.starttls()
        server.ehlo('alocredit.co')
        server.login('noreply@alocredit.co', 'dzxivlyusuprwesu')
        server.send_message(msg, to_addrs=['mdeulofeuth@alocredit.co'])
        print("Success smtp.gmail.com!")
except Exception as e:
    print(f"Error smtp.gmail.com: {e}")
