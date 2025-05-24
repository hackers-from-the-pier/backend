from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.datastructures import UploadFile as FastAPIUploadFile
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional
from pydantic import BaseModel
import uuid
from datetime import datetime
from ftplib import FTP_TLS
import os
from io import BytesIO

from utils.auth import get_current_user
from utils.models import Client, Report, File, User
from utils.database import get_async_session
from utils.config import (
    FTP_HOST,
    FTP_USERNAME,
    FTP_PASSWORD,
    FTP_BASE_URL
)

router = APIRouter(prefix="/report", tags=["Отчеты"])

class ClientResponse(BaseModel):
    id: int
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    is_commercial: Optional[bool] = None
    home_type: Optional[str] = None
    home_area: Optional[float] = None
    season_index: Optional[float] = None
    people_count: Optional[int] = None
    rooms_count: Optional[int] = None
    frod_state: Optional[str] = None
    frod_procentage: Optional[float] = None
    frod_yandex: Optional[str] = None
    frod_avito: Optional[str] = None
    frod_2gis: Optional[str] = None

    class Config:
        from_attributes = True

@router.get("/list", response_model=List[ClientResponse])
async def get_all_clients(
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Получить список всех клиентов
    """
    query = select(Client)
    result = await db.execute(query)
    clients = result.scalars().all()
    return clients

router = APIRouter(prefix="/report", tags=["Отчеты"])

class ReportResponse(BaseModel):
    id: int
    staff_id: Optional[int] = None
    is_ready: bool

    class Config:
        from_attributes = True

class FileResponse(BaseModel):
    id: int
    is_parsed: bool
    report_id: int
    s3_url: str

    class Config:
        from_attributes = True

@router.post("/create", response_model=ReportResponse)
async def create_report(
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Создать новый отчет
    """
    report = Report()
    db.add(report)
    await db.commit()
    await db.refresh(report)
    return report

@router.post("/{report_id}/upload", response_model=FileResponse)
async def upload_file(
    report_id: int,
    file: UploadFile,
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Загрузить файл в отчет через FTPS
    """
    # Проверяем существование отчета
    report = await db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    
    try:
        # Генерируем уникальное имя файла
        file_extension = file.filename.split('.')[-1]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        filename = f"reports/{timestamp}_{unique_id}.{file_extension}"
        
        # Читаем содержимое файла
        file_content = await file.read()
        
        # Подключаемся к FTPS
        ftps = FTP_TLS(FTP_HOST)
        ftps.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
        ftps.prot_p()  # Включаем защищенный режим передачи данных
        
        # Создаем директорию reports, если её нет
        try:
            ftps.mkd('reports')
        except:
            pass  # Директория уже существует
        
        # Загружаем файл
        ftps.storbinary(f'STOR {filename}', BytesIO(file_content))
        ftps.quit()
        
        # Формируем URL для доступа к файлу
        file_url = f"{FTP_BASE_URL}/{filename}"
        
        # Создаем запись о файле в базе данных
        db_file = File(
            report_id=report_id,
            s3_url=file_url  # Используем то же поле, просто меняем URL
        )
        db.add(db_file)
        await db.commit()
        await db.refresh(db_file)
        
        return db_file
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке файла: {str(e)}")

@router.post("/{report_id}/check")
async def start_check(
    report_id: int,
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Запустить проверку отчета
    """
    # Проверяем существование отчета
    report = await db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    
    # Проверяем права доступа
    #if report.staff_id != current_user.id:
    #    raise HTTPException(status_code=403, detail="Нет доступа к этому отчету")
    
    # TODO: Здесь будет логика запуска проверки
    # Например, отправка задачи в очередь или запуск асинхронного процесса
    
    return {"message": "Проверка запущена"}

@router.get("/list", response_model=List[ReportResponse])
async def get_all_reports(
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Получить список всех отчетов пользователя
    """
    query = select(Report)#.where(Report.staff_id == current_user.id)
    result = await db.execute(query)
    reports = result.scalars().all()
    return reports

