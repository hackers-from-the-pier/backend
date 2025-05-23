import uvicorn
import subprocess
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from utils.config import API_HOST, API_PORT, API_VERSION, API_RELOAD

def get_git_commit_id() -> str:
    try:
        # Получаем полный ID текущего коммита
        commit_id = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode("utf-8")
        return commit_id
    except subprocess.CalledProcessError:
        return "unknown"
    
app = FastAPI(
    root_path=f"/api/{API_VERSION}",
    version="#"+get_git_commit_id()[:7],
    title="True Kilowatt API"
)



if __name__ == "__main__":
    uvicorn.run(app="main:app",
                host=API_HOST, 
                port=int(API_PORT),
                reload=API_RELOAD)
    