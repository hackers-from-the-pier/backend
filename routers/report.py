from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.datastructures import UploadFile as FastAPIUploadFile
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from pydantic import BaseModel
import uuid
from datetime import datetime
import os
from pathlib import Path
import json
from data_cleaning.parse_report import process_report
import asyncio
import sys
import subprocess

from utils.auth import get_current_user
from utils.models import Client, Report, File, User
from utils.database import get_async_session

# Конфигурация путей
UPLOAD_DIR = "/var/www/kilowatt/public/file"
BASE_URL = "https://true.kilowattt.ru/file"

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
    Загрузить файл в отчет
    """
    # Проверяем существование отчета
    report = await db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    
    try:
        # Создаем директорию, если её нет
        Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
        
        # Генерируем уникальное имя файла
        file_extension = file.filename.split('.')[-1]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        filename = f"{timestamp}_{unique_id}.{file_extension}"
        
        # Полный путь к файлу
        file_path = os.path.join(UPLOAD_DIR, filename)
        
        # Сохраняем файл
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Формируем URL для доступа к файлу
        file_url = f"{BASE_URL}/{filename}"
        
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

async def update_or_create_client(client: Client, db: AsyncSession) -> Client:
    """
    Обновляет существующего клиента или создает нового
    """
    # Проверяем существование клиента
    query = select(Client).where(Client.id == client.id)
    result = await db.execute(query)
    existing_client = result.scalar_one_or_none()
    
    if existing_client:
        # Обновляем существующего клиента
        for key, value in client.__dict__.items():
            if not key.startswith('_'):
                setattr(existing_client, key, value)
        return existing_client
    else:
        # Создаем нового клиента
        return client

@router.post("/{report_id}/check")
async def start_check(
    report_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Запустить проверку отчета и парсер Авито в отдельном процессе
    """
    # Проверяем существование отчета
    report = await db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    
    # Получаем все файлы отчета
    query = select(File).where(File.report_id == report_id)
    result = await db.execute(query)
    files = result.scalars().all()
    
    if not files:
        raise HTTPException(status_code=404, detail="Файлы отчета не найдены")
    
    try:
        total_clients = 0
        total_consumption = 0
        commercial_clients = 0
        residential_clients = 0
        total_area = 0
        
        # Обрабатываем каждый файл
        for file in files:
            if not file.is_parsed:
                # Получаем путь к файлу из URL
                filename = file.s3_url.split('/')[-1]
                file_path = os.path.join(UPLOAD_DIR, filename)
                
                # Проверяем существование файла
                if not os.path.exists(file_path):
                    continue
                
                # Обрабатываем файл
                clients = process_report(file_path, report_id)
                total_clients += len(clients)
                
                # Собираем статистику
                for client in clients:
                    if hasattr(client, 'is_commercial'):
                        if client.is_commercial:
                            commercial_clients += 1
                        else:
                            residential_clients += 1
                    
                    if hasattr(client, 'home_area'):
                        total_area += client.home_area or 0
                    
                    # Предполагаем, что у клиента есть поле consumption
                    if hasattr(client, 'consumption'):
                        total_consumption += client.consumption or 0
                
                # Сохраняем клиентов в базу данных
                for client in clients:
                    updated_client = await update_or_create_client(client, db)
                    db.add(updated_client)
                
                # Отмечаем файл как обработанный
                file.is_parsed = True
        
        # Обновляем статус отчета
        report.is_ready = True
        report.all_count = total_clients
        
        await db.commit()
        
        # Запускаем парсер Авито в отдельном процессе
        script_path = os.path.join(os.path.dirname(__file__), "..", "avito", "parser_cls.py")
        process = subprocess.Popen([sys.executable, script_path, str(report_id)])
        
        return {
            "message": "Проверка завершена, парсер Авито запущен в отдельном процессе",
            "processed_files": len(files),
            "clients_count": total_clients,
            "parser_process_id": process.pid,
            "statistics": {
                "total_consumption": total_consumption,
                "commercial_clients": commercial_clients,
                "residential_clients": residential_clients,
                "total_area": total_area,
                "average_consumption_per_client": total_consumption / total_clients if total_clients > 0 else 0,
                "average_area_per_client": total_area / total_clients if total_clients > 0 else 0
            }
        }
        
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при обработке отчета: {str(e)}")

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

