from fastapi import APIRouter, Depends

from utils.models import User

router = APIRouter(prefix="/user", tags=["Пользователь"])

@router.get("/me")
async def get_me(user: User = Depends(get_current_user)):
    return user

