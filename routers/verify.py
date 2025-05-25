from fastapi import APIRouter, Depends, HTTPException
from utils.auth import verify_password, access_security
from utils.database import get_async_session, AsyncSession
from utils.models import User
import pydantic
from sqlalchemy import select
from fastapi.responses import StreamingResponse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import io

router = APIRouter(tags=["Проверки"], prefix="/verify")

@router.get("/suspicious-clients-pdf")
async def get_suspicious_clients_pdf(
    db: AsyncSession = Depends(get_async_session)
):
    """
    Генерирует PDF файл с информацией о подозрительных клиентах.
    На каждой странице размещается информация о 6 клиентах.
    """
    # Формируем запрос для получения подозрительных клиентов
    query = select(User).where(
        User.summary_electricity > 3000,
        User.frod_procentage > 0,
        User.is_commercial == False
    )
    
    # Выполняем запрос
    result = await db.execute(query)
    suspicious_clients = result.scalars().all()
    
    # Создаем буфер для PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    
    # Заголовок документа
    title = Paragraph("Отчет по подозрительным клиентам", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 20))
    
    # Разбиваем клиентов на группы по 6 человек
    for i in range(0, len(suspicious_clients), 6):
        page_clients = suspicious_clients[i:i+6]
        
        # Создаем таблицу для текущей страницы
        data = [['ID', 'Имя', 'Email', 'Потребление', 'Процент фрода']]
        for client in page_clients:
            data.append([
                str(client.id),
                client.name,
                client.email,
                str(client.summary_electricity),
                f"{client.frod_procentage}%"
            ])
        
        # Создаем таблицу
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 20))
    
    # Создаем PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=suspicious_clients.pdf"}
    )

