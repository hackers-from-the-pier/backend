from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.datastructures import UploadFile as FastAPIUploadFile
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
import uuid

from utils.auth import get_current_user
from utils.models import Client, Report, File, User
from utils.database import get_async_session
from utils.config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_BUCKET_NAME,
    AWS_ENDPOINT_URL,
    AWS_REGION,
    AWS_VIRTUAL_HOSTED_URL
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
    Загрузить файл в отчет
    """
    # Проверяем существование отчета
    report = await db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    
    # Проверяем права доступа
    #if report.staff_id != current_user.id:
    #    raise HTTPException(status_code=403, detail="Нет доступа к этому отчету")

    # Генерируем уникальное имя файла
    file_extension = file.filename.split('.')[-1]
    unique_filename = f"{uuid.uuid4()}.{file_extension}"
    
    # Загружаем файл в S3 Beget
    s3_client = boto3.client(
        's3',
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=boto3.session.Config(
            s3={'addressing_style': 'virtual'},
            signature_version='s3v4',
            s3={'payload_signing_enabled': True}
        )
    )
    
    try:
        # Сбрасываем указатель файла в начало
        await file.seek(0)
        
        s3_client.upload_fileobj(
            file.file,
            AWS_BUCKET_NAME,
            unique_filename,
            ExtraArgs={'ContentType': file.content_type}
        )
        # Используем virtual hosted style URL
        s3_url = f"{AWS_VIRTUAL_HOSTED_URL}/{unique_filename}"
        
        # Создаем запись о файле в базе данных
        db_file = File(
            report_id=report_id,
            s3_url=s3_url
        )
        db.add(db_file)
        await db.commit()
        await db.refresh(db_file)
        
        return db_file
        
    except ClientError as e:
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

