"""
Servicio para envio de correos electronicos con informes de asignacion.
"""
import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


class EmailService:
    """Servicio de envio de correos electronicos."""

    def __init__(self):
        self.smtp_server = settings.SMTP_SERVER
        self.smtp_port = int(settings.SMTP_PORT)
        self.helo_name = settings.SMTP_HELO_NAME
        self.email_user = settings.SMTP_USER
        self.email_password = settings.SMTP_PASSWORD
        self.email_from = settings.SMTP_FROM

        # Destinatarios de excepcion: Serlefin tambien se envia con Excel.
        self.serlefin_attachment_exception_recipients = set(
            settings.serlefin_attachment_exception_recipients
        )

    def _recipient_requires_serlefin_attachment(self, recipient: str) -> bool:
        """Retorna True si el destinatario esta en la lista de excepcion."""
        return recipient.strip().lower() in self.serlefin_attachment_exception_recipients

    def send_assignment_report(
        self,
        recipient: str,
        subject: str,
        body: str,
        attachments: Optional[List[str]] = None,
    ) -> bool:
        """
        Envia un correo con informes de asignacion.

        Args:
            recipient: Correo del destinatario
            subject: Asunto del correo
            body: Cuerpo del mensaje (HTML)
            attachments: Lista de rutas de archivos a adjuntar

        Returns:
            bool: True si el envio fue exitoso, False en caso contrario
        """
        try:
            msg = MIMEMultipart()
            msg["From"] = self.email_from
            msg["To"] = recipient
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "html"))

            if attachments:
                for file_path in attachments:
                    if not Path(file_path).exists():
                        logger.warning(f"Archivo no encontrado: {file_path}")
                        continue

                    with open(file_path, "rb") as file_handle:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(file_handle.read())

                    encoders.encode_base64(part)
                    filename = Path(file_path).name
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename={filename}",
                    )
                    msg.attach(part)

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo(self.helo_name)
                server.starttls()
                server.ehlo(self.helo_name)
                server.login(self.email_user, self.email_password)
                server.send_message(msg)

            logger.info(f"Correo enviado exitosamente a {recipient}")
            return True

        except Exception as error:
            logger.error(f"Error al enviar correo: {error}")
            return False

    def send_multiple_reports(
        self,
        recipient: str,
        serlefin_file: str,
        cobyser_file: str,
        metrics_html: str,
        attach_serlefin_file: bool = False,
        attach_cobyser_file: bool = True,
    ) -> bool:
        """
        Envia informes de ambas casas de cobranza.

        Args:
            recipient: Correo del destinatario
            serlefin_file: Ruta del archivo Excel de Serlefin
            cobyser_file: Ruta del archivo Excel de Cobyser
            metrics_html: HTML con metricas de asignacion
            attach_serlefin_file: Si True adjunta Excel de Serlefin
            attach_cobyser_file: Si True adjunta Excel de Cobyser

        Returns:
            bool: True si el envio fue exitoso
        """
        serlefin_file_exists = bool(serlefin_file) and Path(serlefin_file).exists()
        cobyser_file_exists = bool(cobyser_file) and Path(cobyser_file).exists()

        # Regla general: Serlefin sin adjunto. Excepcion por destinatario.
        effective_attach_serlefin = (
            attach_serlefin_file
            or self._recipient_requires_serlefin_attachment(recipient)
        )
        effective_attach_cobyser = attach_cobyser_file

        if effective_attach_serlefin and serlefin_file_exists:
            logger.info(
                f"Serlefin se enviara con adjunto para destinatario de excepcion: {recipient}"
            )

        serlefin_attachment_text = (
            "Excel adjunto"
            if effective_attach_serlefin and serlefin_file_exists
            else "solo mensaje HTML (sin Excel adjunto)"
        )
        cobyser_attachment_text = (
            "Excel adjunto"
            if effective_attach_cobyser and cobyser_file_exists
            else "solo mensaje HTML (sin Excel adjunto)"
        )

        subject = "Informes de Asignacion de Cartera - Serlefin y Cobyser"

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
                <h1>Informes de Asignacion de Cartera</h1>
            </div>
            <div class="content">
                <p>Estimado/a,</p>
                <p>Se comparte el resumen de asignacion de cartera para Serlefin y Cobyser.</p>

                <div class="metrics">
                    {metrics_html}
                </div>

                <p><strong>Serlefin:</strong> ya puedes validar en tu plataforma segura la informacion.</p>

                <p><strong>Archivos adjuntos:</strong></p>
                <ul>
                    <li>Serlefin (Usuario 81): {serlefin_attachment_text}</li>
                    <li>Cobyser (Usuario 45): {cobyser_attachment_text}</li>
                </ul>

                <p>Los informes incluyen la columna <strong>Contrato Fijo</strong>.</p>
            </div>
            <div class="footer">
                <p>Este es un correo automatico generado por el Sistema de Asignacion de Cartera AloCredit.</p>
                <p>Por favor, no responder a este correo.</p>
            </div>
        </body>
        </html>
        """

        attachments: List[str] = []
        if effective_attach_serlefin and serlefin_file_exists:
            attachments.append(serlefin_file)
        if effective_attach_cobyser and cobyser_file_exists:
            attachments.append(cobyser_file)

        return self.send_assignment_report(
            recipient=recipient,
            subject=subject,
            body=body,
            attachments=attachments,
        )


# Instancia global del servicio
email_service = EmailService()

