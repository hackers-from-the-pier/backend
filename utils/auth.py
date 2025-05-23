import json
import uuid
import hmac
import hashlib
from urllib.parse import parse_qs
from jose import jwt, JWTError, ExpiredSignatureError
import passlib.context
from typing import Annotated, Optional
from datetime import timedelta
from fastapi_jwt import JwtAccessBearer
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from utils.database import get_async_session, AsyncSession
from pydantic import BaseModel

from utils.config import JWT_SECRET, BOT_TOKEN
from utils.models import ExpToken, User
from sqlalchemy import select, or_

login_method_url = "/auth/jwt"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=login_method_url + "/login")
pwd_context = passlib.context.CryptContext(schemes=["bcrypt"], deprecated="auto")

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], 
                           session: AsyncSession = Depends(get_async_session)):
    try: 
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except ExpiredSignatureError: 
        er_id = uuid.uuid4().__str__()
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Token expired. Trace UUID: " + er_id)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    exp_token = await session.get(ExpToken, payload.get("jti"))
    if exp_token: 
        er_id = uuid.uuid4().__str__()
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Token expired. Trace UUID: " + er_id)
    
    user_id = payload.get("subject", {}).get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token format",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = await session.get(User, int(user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.is_verified:
        user.is_verified = True
        await session.commit()
    
    return user

async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)
