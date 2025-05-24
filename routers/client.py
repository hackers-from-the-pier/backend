from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from utils.auth import get_current_user
from utils.models import Client
from utils.database import get_async_session

router = APIRouter(prefix="/client", tags=["Клиенты"])

@router.get("/list", response_model=List[Client])
async def get_all_clients(
    db: Session = Depends(get_async_session),
    #current_user: User = Depends(get_current_user)
):
    """
    Получить список всех клиентов
    """
    clients = db.query(Client).all()
    return clients

