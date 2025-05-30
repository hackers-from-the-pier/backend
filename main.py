import uvicorn
import subprocess
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.user import router as user_router
from routers.auth import router as auth_router
from routers.client import router as client_router
from routers.report import router as report_router
from routers.verify import router as verify_router

from utils.config import API_HOST, API_PORT, API_VERSION, API_RELOAD

def get_git_commit_id() -> str:
    try:
        # Получаем полный ID текущего коммита
        commit_id = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode("utf-8")
        return commit_id
    except subprocess.CalledProcessError:
        return "unknown"
    
app = FastAPI(
    root_path=f"/{API_VERSION}",
    version="#"+get_git_commit_id()[:7],
    title="True Kilowatt API"
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://true.kilowattt.ru"],  # Разрешаем запросы с этого домена
    allow_credentials=True,
    allow_methods=["*"],  # Разрешаем все HTTP методы
    allow_headers=["*"],  # Разрешаем все заголовки
)

app.include_router(user_router)
app.include_router(auth_router)
app.include_router(client_router)
app.include_router(report_router)
app.include_router(verify_router)

if __name__ == "__main__":
    uvicorn.run(app="main:app",
                host=API_HOST, 
                port=int(API_PORT),
                reload=API_RELOAD)
    