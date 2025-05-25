from fastapi import APIRouter, Depends, HTTPException
from utils.auth import verify_password, access_security
from utils.database import get_async_session, AsyncSession
from utils.models import Client
import pydantic
from sqlalchemy import select
from fastapi.responses import StreamingResponse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
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
    query = select(Client).where(
        Client.summary_electricity > 3000,
        Client.frod_procentage > 0,
        Client.is_commercial == False
    )
    
    # Выполняем запрос
    result = await db.execute(query)
    suspicious_clients = result.scalars().all()
    
    # Создаем буфер для PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4))  # Используем альбомную ориентацию
    elements = []
    
    # Создаем стили
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='CustomTitle',
        fontName='Times-Roman',
        fontSize=16,
        alignment=1,
        spaceAfter=30
    ))
    styles.add(ParagraphStyle(
        name='CustomNormal',
        fontName='Times-Roman',
        fontSize=12,
        alignment=0
    ))
    
    # Заголовок документа
    title = Paragraph("Отчет по подозрительным клиентам", styles['CustomTitle'])
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
                client.name or "Нет имени",
                client.email or "Нет email",
                str(client.summary_electricity or 0),
                f"{client.frod_procentage or 0}%"
            ])
        
        # Создаем таблицу с фиксированными размерами колонок
        col_widths = [50, 150, 200, 100, 100]  # Ширина каждой колонки
        table = Table(data, colWidths=col_widths)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, -1), 'Times-Roman'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('WORDWRAP', (0, 0), (-1, -1), True),  # Включаем перенос слов
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 20))
        
        # Добавляем разрыв страницы после каждой таблицы, кроме последней
        if i + 6 < len(suspicious_clients):
            elements.append(PageBreak())
    
    # Создаем PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=suspicious_clients.pdf"}
    )

