from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional
from pydantic import BaseModel

from utils.auth import get_current_user
from utils.models import Client
from utils.database import get_async_session

router = APIRouter(prefix="/client", tags=["Клиенты"])

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

@router.get("/list")
async def get_all_clients(
    offset: int = 0,
    limit: int = 10,
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Получить список всех клиентов с пагинацией
    """
    # Получаем общее количество клиентов
    total_query = select(Client)
    total_result = await db.execute(total_query)
    total_clients = len(total_result.scalars().all())
    
    # Вычисляем общее количество страниц
    total_pages = (total_clients + limit - 1) // limit
    
    # Получаем клиентов с пагинацией
    query = select(Client).offset(offset).limit(limit)
    result = await db.execute(query)
    clients = result.scalars().all()
    
    return {
        "clients": clients,
        "total_pages": total_pages,
        "current_page": offset // limit + 1,
        "total_clients": total_clients
    }
