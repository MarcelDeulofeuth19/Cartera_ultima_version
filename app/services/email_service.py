"""
Servicio para envío de correos electrónicos con informes de asignación
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class EmailService:
    """Servicio de envío de correos electrónicos"""
    
    def __init__(self):
        self.smtp_server = "smtp-relay.gmail.com"
        self.smtp_port = 587
        self.helo_name = "alocredit.co"
        self.email_user = "noreply@alocredit.co"
        self.email_password = "dzxivlyusuprwesu"
        self.email_from = "noreply@alocredit.co"
    
    def send_assignment_report(
        self,
        recipient: str,
        subject: str,
        body: str,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Envía un correo con informes de asignación
        
        Args:
            recipient: Correo del destinatario
            subject: Asunto del correo
            body: Cuerpo del mensaje (HTML)
            attachments: Lista de rutas de archivos a adjuntar
        
        Returns:
            bool: True si el envío fue exitoso, False en caso contrario
        """
        try:
            # Crear mensaje
            msg = MIMEMultipart()
            msg['From'] = self.email_from
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Agregar cuerpo HTML
            msg.attach(MIMEText(body, 'html'))
            
            # Agregar archivos adjuntos
            if attachments:
                for file_path in attachments:
                    if not Path(file_path).exists():
                        logger.warning(f"Archivo no encontrado: {file_path}")
                        continue
                    
                    with open(file_path, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                    
                    encoders.encode_base64(part)
                    filename = Path(file_path).name
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {filename}'
                    )
                    msg.attach(part)
            
            # Conectar y enviar
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo(self.helo_name)
                server.starttls()
                server.ehlo(self.helo_name)
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
            
            logger.info(f"✅ Correo enviado exitosamente a {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al enviar correo: {e}")
            return False
    
    def send_multiple_reports(
        self,
        recipient: str,
        serlefin_file: str,
        cobyser_file: str,
        metrics_html: str
    ) -> bool:
        """
        Envía informes de ambas casas de cobranza
        
        Args:
            recipient: Correo del destinatario
            serlefin_file: Ruta del archivo Excel de Serlefin
            cobyser_file: Ruta del archivo Excel de Cobyser
            metrics_html: HTML con métricas de asignación
        
        Returns:
            bool: True si el envío fue exitoso
        """
        subject = "📊 Informes de Asignación de Cartera - Serlefin y Cobyser"
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .metrics {{ background-color: #f0f0f0; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .footer {{ text-align: center; padding: 20px; color: #777; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Informes de Asignación de Cartera</h1>
            </div>
            <div class="content">
                <p>Estimado/a,</p>
                <p>Se adjuntan los informes de asignación de cartera para las casas de cobranza <strong>Serlefin</strong> y <strong>Cobyser</strong>.</p>
                
                <div class="metrics">
                    {metrics_html}
                </div>
                
                <p><strong>Archivos adjuntos:</strong></p>
                <ul>
                    <li>Informe Serlefin (Usuario 81)</li>
                    <li>Informe Cobyser (Usuario 45)</li>
                </ul>
                
                <p>Los informes incluyen la columna <strong>"Contrato Fijo"</strong> que indica si el contrato es una base fija o variable.</p>
            </div>
            <div class="footer">
                <p>Este es un correo automático generado por el Sistema de Asignación de Cartera AloCredit.</p>
                <p>Por favor, no responder a este correo.</p>
            </div>
        </body>
        </html>
        """
        
        attachments = []
        if Path(serlefin_file).exists():
            attachments.append(serlefin_file)
        if Path(cobyser_file).exists():
            attachments.append(cobyser_file)
        
        return self.send_assignment_report(recipient, subject, body, attachments)


# Instancia global del servicio
email_service = EmailService()
