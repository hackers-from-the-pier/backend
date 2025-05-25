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
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from notifiers import get_notifier
import io
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

router = APIRouter(tags=["Проверки"], prefix="/verify")

# SMTP настройки
SMTP_SETTINGS = {
    'host': 'smtp.beget.com',
    'port': 587,
    'username': 'info@kilowattt.ru',
    'password': '8oXBi2i297R*',
    'tls': True
}

# Путь к файлу шрифта
FONT_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "static", "fonts", "DejaVuSans-Regular.ttf")

# Регистрируем шрифт с поддержкой кириллицы
try:
    pdfmetrics.registerFont(TTFont('DejaVuSans', FONT_FILE_PATH))
    # Регистрируем тот же файл для жирного начертания
    pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', FONT_FILE_PATH))
    DEFAULT_FONT = 'DejaVuSans'
except Exception as e:
    print(f"Ошибка при загрузке шрифтов DejaVuSans: {e}")
    # Если шрифты не найдены или ошибка, используем встроенный шрифт (может не поддерживать кириллицу)
    DEFAULT_FONT = 'Helvetica'
    # Для встроенных шрифтов не нужна регистрация TTFont, они уже есть

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
        fontName=DEFAULT_FONT,
        fontSize=16,
        alignment=1,
        spaceAfter=30
    ))
    styles.add(ParagraphStyle(
        name='CustomNormal',
        fontName=DEFAULT_FONT,
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
        data = [['ID', 'Имя', 'Email', 'Среднее потребление в месяц (кВт⋅ч)', 'Процент фрода']]
        for client in page_clients:
            data.append([
                str(client.id),
                client.name or "Нет имени",
                client.email or "Нет email",
                str(client.avg_monthly_electricity or 0),
                f"{client.frod_procentage or 0}%"
            ])
        
        # Создаем таблицу с фиксированными размерами колонок
        # Скорректированы ширины для лучшего размещения
        available_width = landscape(A4)[0] - doc.leftMargin - doc.rightMargin
        col_widths = [
            available_width * 0.08, # ID (8%)
            available_width * 0.22, # Имя (22%)
            available_width * 0.42, # Email (42%)
            available_width * 0.12, # Потребление (12%)
            available_width * 0.16  # Процент фрода (16%)
        ]

        table = Table(data, colWidths=col_widths)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'), # Выравнивание по левому краю для данных
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'), # Выравнивание заголовков по центру
            ('FONTNAME', (0, 0), (-1, -1), DEFAULT_FONT), # Используем основной шрифт для всех ячеек
            ('FONTNAME', (0, 0), (-1, 0), f'{DEFAULT_FONT}-Bold'), # Используем жирный шрифт для заголовков
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'), # Вертикальное выравнивание по верхнему краю
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('WORDWRAP', (0, 0), (-1, -1), True),  # Включаем перенос слов
        ]))
        
        elements.append(table)
        
        # Добавляем разрыв страницы после каждой таблицы, кроме последней
        if i + 6 < len(suspicious_clients):
            elements.append(Spacer(1, 30)) # Добавляем немного пространства перед разрывом
            elements.append(PageBreak())
            elements.append(Spacer(1, 30)) # Добавляем немного пространства после разрыва
    
    # Создаем PDF
    doc.build(elements)
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=suspicious_clients.pdf"}
    )

@router.post("/send-notification/{client_id}")
async def send_client_notification(
    client_id: int,
    db: AsyncSession = Depends(get_async_session)
):
    """
    Отправляет уведомление клиенту о подозрительной активности
    """
    # Проверяем существование клиента
    query = select(Client).where(
        Client.id == client_id,
        Client.summary_electricity > 3000,
        Client.frod_procentage > 0,
        Client.is_commercial == False
    )
    result = await db.execute(query)
    client = result.scalar_one_or_none()
    
    if not client:
        raise HTTPException(status_code=404, detail="Клиент не найден или не является подозрительным")
    
    if not client.email:
        raise HTTPException(status_code=400, detail="У клиента не указан email")
    
    try:
        # Создаем экземпляр email notifier
        email = get_notifier('email')
        
        # Формируем текст письма
        subject = "Уведомление о подозрительной активности"
        message = f"""
        Уважаемый(ая) {client.name or 'клиент'}!
        
        Мы обнаружили подозрительную активность в вашем энергопотреблении.
        Согласно нашим данным, ваше потребление превышает нормативные показатели для физических лиц.
        
        Рекомендуем вам перейти на тарифы для юридических лиц.
        Для этого, пожалуйста, свяжитесь с нашим отделом обслуживания клиентов.
        
        С уважением,
        Команда энергосбытовой компании
        """
        
        # Отправляем письмо
        response = email.notify(
            to=client.email,
            subject=subject,
            message=message,
            **SMTP_SETTINGS
        )
        
        if not response.ok:
            raise HTTPException(status_code=500, detail="Ошибка при отправке уведомления")
        
        return {"message": "Уведомление успешно отправлено"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при отправке уведомления: {str(e)}")

@router.post("/send-notifications-bulk")
async def send_notifications_bulk(
    db: AsyncSession = Depends(get_async_session)
):
    """
    Отправляет уведомления всем подозрительным клиентам
    """
    # Получаем всех подозрительных клиентов
    query = select(Client).where(
        Client.summary_electricity > 3000,
        Client.frod_procentage > 0,
        Client.is_commercial == False,
        Client.email.isnot(None)  # Только клиенты с email
    )
    result = await db.execute(query)
    suspicious_clients = result.scalars().all()
    
    if not suspicious_clients:
        return {"message": "Нет подозрительных клиентов для отправки уведомлений"}
    
    success_count = 0
    failed_count = 0
    
    for client in suspicious_clients:
        try:
            # Создаем экземпляр email notifier
            email = get_notifier('email')
            
            # Формируем текст письма
            subject = "Уведомление о подозрительной активности"
            message = f"""
            Уважаемый(ая) {client.name or 'клиент'}!
            
            Мы обнаружили подозрительную активность в вашем энергопотреблении.
            Согласно нашим данным, ваше потребление превышает нормативные показатели для физических лиц.
            
            Рекомендуем вам перейти на тарифы для юридических лиц.
            Для этого, пожалуйста, свяжитесь с нашим отделом обслуживания клиентов.
            
            С уважением,
            Команда энергосбытовой компании
            """
            
            # Отправляем письмо
            response = email.notify(
                to=client.email,
                subject=subject,
                message=message,
                **SMTP_SETTINGS
            )
            
            if response.ok:
                success_count += 1
            else:
                failed_count += 1
                
        except Exception:
            failed_count += 1
            continue
    
    return {
        "message": "Массовая рассылка завершена",
        "success_count": success_count,
        "failed_count": failed_count
    }

@router.get("/notification-pdf/{client_id}")
async def get_notification_pdf(
    client_id: int,
    db: AsyncSession = Depends(get_async_session)
):
    """
    Генерирует PDF файл с текстом уведомления для клиента
    """
    # Проверяем существование клиента
    query = select(Client).where(
        Client.id == client_id,
        Client.summary_electricity > 3000,
        Client.frod_procentage > 0,
        Client.is_commercial == False
    )
    result = await db.execute(query)
    client = result.scalar_one_or_none()
    
    if not client:
        raise HTTPException(status_code=404, detail="Клиент не найден или не является подозрительным")
    
    # Создаем буфер для PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    
    # Создаем стили
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='CustomTitle',
        fontName=DEFAULT_FONT,
        fontSize=16,
        alignment=1,
        spaceAfter=30
    ))
    styles.add(ParagraphStyle(
        name='CustomNormal',
        fontName=DEFAULT_FONT,
        fontSize=12,
        alignment=0,
        spaceAfter=12
    ))
    
    # Заголовок документа
    title = Paragraph("Уведомление о подозрительной активности", styles['CustomTitle'])
    elements.append(title)
    elements.append(Spacer(1, 20))
    
    # Дата
    current_date = datetime.now().strftime("%d.%m.%Y")
    date_paragraph = Paragraph(f"Дата: {current_date}", styles['CustomNormal'])
    elements.append(date_paragraph)
    elements.append(Spacer(1, 20))
    
    # Адресат
    recipient = Paragraph(f"Уважаемый(ая) {client.name or 'клиент'}!", styles['CustomNormal'])
    elements.append(recipient)
    elements.append(Spacer(1, 20))
    
    # Основной текст
    main_text = """
    Мы обнаружили подозрительную активность в вашем энергопотреблении.
    Согласно нашим данным, ваше потребление превышает нормативные показатели для физических лиц.
    
    Рекомендуем вам перейти на тарифы для юридических лиц.
    Для этого, пожалуйста, свяжитесь с нашим отделом обслуживания клиентов.
    """
    main_paragraph = Paragraph(main_text, styles['CustomNormal'])
    elements.append(main_paragraph)
    elements.append(Spacer(1, 40))
    
    # Подпись
    signature = """
    С уважением,
    Команда энергосбытовой компании
    """
    signature_paragraph = Paragraph(signature, styles['CustomNormal'])
    elements.append(signature_paragraph)
    
    # Создаем PDF
    doc.build(elements)
    buffer.seek(0)
    
    # Формируем имя файла
    filename = f"notification_{client_id}_{current_date}.pdf"
    
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

