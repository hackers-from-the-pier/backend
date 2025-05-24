from fastapi import APIRouter, Depends, HTTPException
from utils.auth import verify_password, access_security
from utils.database import get_async_session, AsyncSession
from utils.models import User
import pydantic
from sqlalchemy import select

router = APIRouter(tags=["Авторизация"], prefix="/auth")

class LoginData(pydantic.BaseModel):
    email: str
    password: str

@router.post("/jwt", response_model=dict, summary="Авторизация через JWT")
async def jwt_auth(data: LoginData, session: AsyncSession = Depends(get_async_session)):
    
    user = await session.execute(select(User).where(User.email == data.email))
    user = user.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    if not verify_password(data.password, user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    access_token = access_security.create_access_token(
        subject={"user_id": str(user.id), "role": user.role}
    )
    
    return {
        "message": "Successfully authenticated",
        "access_token": access_token,
        "token_type": "bearer"
    }
