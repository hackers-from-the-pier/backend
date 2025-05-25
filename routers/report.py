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
import logging

from utils.auth import get_current_user
from utils.models import Client, Report, File, User
from utils.database import get_async_session

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    #background_tasks: BackgroundTasks,
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Запустить проверку отчета и парсеры в фоновом режиме
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

    async def process_files():
        try:
            logger.info(f"Начало обработки файлов для отчета {report_id}")
            total_clients = 0
            total_consumption = 0
            commercial_clients = 0
            residential_clients = 0
            total_area = 0
            processed_files = 0
            failed_files = 0
            
            # Создаем список задач для асинхронной обработки
            tasks = []
            
            # Обрабатываем каждый файл
            for file in files:
                if not file.is_parsed:
                    # Получаем путь к файлу из URL
                    filename = file.s3_url.split('/')[-1]
                    file_path = os.path.join(UPLOAD_DIR, filename)
                    logger.info(f"Подготовка к обработке файла: {filename}")
                    
                    # Проверяем существование файла
                    if not os.path.exists(file_path):
                        logger.error(f"Файл не найден: {file_path}")
                        failed_files += 1
                        continue
                    
                    # Создаем задачу для обработки файла
                    async def process_single_file(file_path, file_id):
                        try:
                            logger.info(f"Начало обработки файла: {file_path}")
                            # Запускаем process_report в отдельном потоке
                            loop = asyncio.get_event_loop()
                            logger.info(f"Запуск process_report для файла: {file_path}")
                            clients = await loop.run_in_executor(None, process_report, file_path, report_id)
                            logger.info(f"Файл обработан, получено клиентов: {len(clients)}")
                            
                            # Обновляем статистику
                            nonlocal total_clients, total_consumption, commercial_clients, residential_clients, total_area, processed_files
                            total_clients += len(clients)
                            processed_files += 1
                            
                            # Собираем статистику
                            for client in clients:
                                if hasattr(client, 'is_commercial'):
                                    if client.is_commercial:
                                        commercial_clients += 1
                                    else:
                                        residential_clients += 1
                                
                                if hasattr(client, 'home_area'):
                                    total_area += client.home_area or 0
                                
                                if hasattr(client, 'consumption'):
                                    total_consumption += client.consumption or 0
                            
                            logger.info(f"Сохранение клиентов в базу данных для файла: {file_path}")
                            # Сохраняем клиентов в базу данных
                            for client in clients:
                                updated_client = await update_or_create_client(client, db)
                                db.add(updated_client)
                            
                            # Отмечаем файл как обработанный
                            file.is_parsed = True
                            logger.info(f"Файл успешно обработан: {file_path}")
                            return True
                        except Exception as e:
                            logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
                            nonlocal failed_files
                            failed_files += 1
                            return False
                    
                    tasks.append(process_single_file(file_path, file.id))
            
            # Запускаем все задачи параллельно
            if tasks:
                logger.info(f"Запуск параллельной обработки {len(tasks)} файлов")
                await asyncio.gather(*tasks)
                logger.info("Параллельная обработка завершена")
            
            # Обновляем статус отчета
            logger.info("Обновление статуса отчета")
            report = await db.get(Report, report_id)
            if report:
                report.is_ready = True
                report.all_count = total_clients
                await db.commit()
                logger.info("Статус отчета обновлен")
            
            logger.info(f"Обработка завершена. Статистика: обработано файлов: {processed_files}, ошибок: {failed_files}")
            return {
                "status": "success",
                "statistics": {
                    "total_clients": total_clients,
                    "total_consumption": total_consumption,
                    "commercial_clients": commercial_clients,
                    "residential_clients": residential_clients,
                    "total_area": total_area,
                    "processed_files": processed_files,
                    "failed_files": failed_files
                }
            }
            
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке отчета {report_id}: {str(e)}")
            # Обновляем статус отчета в случае ошибки
            report = await db.get(Report, report_id)
            if report:
                report.is_ready = False
                await db.commit()
            return {
                "status": "error",
                "error": str(e),
                "statistics": {
                    "total_clients": total_clients,
                    "total_consumption": total_consumption,
                    "commercial_clients": commercial_clients,
                    "residential_clients": residential_clients,
                    "total_area": total_area,
                    "processed_files": processed_files,
                    "failed_files": failed_files
                }
            }

    logger.info(f"Запуск обработки отчета {report_id}")
    result = await process_files()
    logger.info(f"Обработка отчета {report_id} завершена со статусом: {result['status']}")
    
    # Сразу возвращаем ответ
    return {
        "message": "Проверка завершена",
        "report_id": report_id,
        "status": result["status"],
        "statistics": result["statistics"]
    }

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

