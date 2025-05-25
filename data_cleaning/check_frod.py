import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from utils.database import get_async_session
from utils.models import Client
from .parse_report import generate_2gis_url

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def check_client_frod(client: Client, db: AsyncSession):
    """
    Проверяет одного клиента на фрод
    """
    try:
        logger.info(f"Проверка клиента {client.id} с адресом: {client.address}")
        
        # Проверяем 2GIS
        z2gis = generate_2gis_url(client.address)
        
        # Обновляем данные клиента
        client.frod_2gis = z2gis
        client.frod_procentage = 30 if z2gis else 0
        client.frod_state = "Проверен"
        
        await db.commit()
        logger.info(f"Клиент {client.id} проверен, результат: {client.frod_state}")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке клиента {client.id}: {str(e)}", exc_info=True)
        client.frod_state = "Ошибка проверки"
        await db.commit()

async def check_pending_clients():
    """
    Проверяет всех клиентов со статусом "Оценивается"
    """
    logger.info("Начало проверки клиентов")
    try:
        async for db in get_async_session():
            # Получаем всех клиентов со статусом "Оценивается"
            query = select(Client).where(Client.frod_state == "Оценивается")
            result = await db.execute(query)
            clients = result.scalars().all()
            
            if not clients:
                logger.info("Нет клиентов для проверки")
                return
            
            logger.info(f"Найдено {len(clients)} клиентов для проверки")
            
            # Создаем задачи для проверки каждого клиента
            tasks = [check_client_frod(client, db) for client in clients]
            
            # Запускаем проверку параллельно
            await asyncio.gather(*tasks)
            
            logger.info("Проверка клиентов завершена")
            
    except Exception as e:
        logger.error(f"Ошибка при проверке клиентов: {str(e)}", exc_info=True)

async def start_frod_checker():
    """
    Запускает бесконечный цикл проверки клиентов
    """
    while True:
        await check_pending_clients()
        await asyncio.sleep(60)  # Ждем 1 минуту перед следующей проверкой 