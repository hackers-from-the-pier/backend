import json
from typing import List, Dict, Any, Optional, Union
from faker import Faker
import pandas as pd
import numpy as np
import requests
from utils.models import Client
import urllib.parse
from .fill_missing import fill_missing_by_group
import logging
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import tempfile
import os
import uuid
import threading
import time
import atexit

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Устанавливаем опцию для будущего поведения pandas
pd.set_option('future.no_silent_downcasting', True)

class SeleniumDriver:
    _instance = None
    _lock = threading.Lock()
    _driver = None
    _is_initialized = False
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        if not self._is_initialized:
            self._initialize_driver()
            self._is_initialized = True
            atexit.register(self.cleanup)
    
    def _initialize_driver(self):
        """Инициализация драйвера Chrome"""
        temp_dir = os.path.join(tempfile.gettempdir(), 'chrome_profile')
        os.makedirs(temp_dir, exist_ok=True)
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_argument(f'--user-data-dir={temp_dir}')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        
        try:
            self._driver = webdriver.Chrome(options=chrome_options)
            logger.info("Драйвер Chrome успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка при инициализации драйвера: {str(e)}")
            raise
    
    def get_driver(self):
        """Получение экземпляра драйвера"""
        if self._driver is None:
            self._initialize_driver()
        return self._driver
    
    def cleanup(self):
        """Очистка ресурсов драйвера"""
        if self._driver:
            try:
                self._driver.quit()
                logger.info("Драйвер Chrome успешно закрыт")
            except:
                pass
            finally:
                self._driver = None
                self._is_initialized = False

def is_nan_or_none(value: Any) -> bool:
    """
    Безопасная проверка на NaN или None
    """
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return pd.isna(value) or np.isnan(value)
    return False

def convert_value(value: Any, target_type: type) -> Optional[Any]:
    """
    Конвертирует значение в нужный тип с обработкой ошибок
    """
    if is_nan_or_none(value):
        return None
    
    try:
        if target_type == bool:
            return bool(value)
        elif target_type == int:
            return int(float(str(value).replace(',', '.')))
        elif target_type == float:
            return float(str(value).replace(',', '.'))
        else:
            return target_type(value)
    except (ValueError, TypeError):
        return None

def generate_2gis_url(address: str) -> str:
    """
    Генерирует URL для поиска адреса в 2GIS и проверяет его через Selenium
    """
    if not address:
        return None
        
    logger.info(f"Генерация 2GIS URL для адреса: {address}")
    
    # Кодируем адрес для URL
    encoded_address = quote(address)
    url = f"https://2gis.ru/novorossiysk/search/{encoded_address}"
    
    try:
        # Получаем экземпляр драйвера
        driver_manager = SeleniumDriver.get_instance()
        driver = driver_manager.get_driver()
        
        logger.info(f"Отправка запроса к 2GIS для адреса: {address}")
        
        try:
            driver.get(url)
            
            # Ждем загрузки результатов поиска
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "searchResults__list"))
            )
            
            # Получаем текст страницы
            page_text = driver.page_source.lower()
            
            # Проверяем наличие ключевых фраз
            hotel_phrases = [
                'гостиница', 'отель', 'хостел', 'апартаменты',
                'сдается', 'аренда', 'проживание', 'номер',
                'почасовая', 'посуточная', 'мини-отель'
            ]
            
            for phrase in hotel_phrases:
                if phrase in page_text:
                    logger.info(f"Найдено совпадение с фразой: {phrase}")
                    return url
                    
            logger.info("Совпадений не найдено")
            return None
            
        except TimeoutException:
            logger.error("Таймаут при загрузке страницы")
            return None
        except WebDriverException as e:
            logger.error(f"Ошибка Selenium: {str(e)}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при проверке адреса {address}: {str(e)}")
        return None

def parse_client_data(client_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает данные клиента из JSON и преобразует их в формат модели Client
    с обработкой пропущенных значений и конвертацией типов
    """
    logger.info(f"Начало парсинга данных клиента: {client_data.get('accountId', 'Unknown')}")
    
    # Маппинг полей из входного JSON в поля модели
    field_mapping = {
        'accountId': 'id',
        'address': 'address',
        'buildingType': 'home_type',
        'roomsCount': 'rooms_count',
        'residentsCount': 'people_count',
        'totalArea': 'home_area'
    }
    
    parsed_data = {}
    
    # Обрабатываем основные поля
    for json_field, model_field in field_mapping.items():
        value = client_data.get(json_field)
        if json_field in ['roomsCount', 'residentsCount']:
            # Для целочисленных полей
            if value is not None:
                try:
                    parsed_data[model_field] = int(float(str(value).replace(',', '.')))
                except (ValueError, TypeError):
                    parsed_data[model_field] = None
            else:
                parsed_data[model_field] = None
        elif json_field == 'totalArea':
            # Для полей с плавающей точкой
            if value is not None:
                try:
                    # Округляем до 2 знаков после запятой
                    parsed_data[model_field] = round(float(str(value).replace(',', '.')), 2)
                except (ValueError, TypeError):
                    parsed_data[model_field] = None
            else:
                parsed_data[model_field] = None
        else:
            parsed_data[model_field] = value
    
    # Обработка данных о потреблении
    consumption = client_data.get('consumption', {})
    if consumption:
        monthly_values = [float(v) for v in consumption.values() if v is not None]
        if monthly_values:
            # Расчет показателей потребления
            parsed_data['summary_electricity'] = sum(monthly_values)
            parsed_data['avg_monthly_electricity'] = sum(monthly_values) / len(monthly_values)
            parsed_data['max_monthly_electricity'] = max(monthly_values)
            parsed_data['min_monthly_electricity'] = min(monthly_values)
            
            # Расчет потребления на квадратный метр и на человека
            if parsed_data.get('home_area'):
                parsed_data['electricity_per_sqm'] = parsed_data['summary_electricity'] / parsed_data['home_area']
            if parsed_data.get('people_count'):
                parsed_data['electricity_per_person'] = parsed_data['summary_electricity'] / parsed_data['people_count']
            
            # Определение коммерческого статуса
            parsed_data['is_commercial'] = parsed_data['avg_monthly_electricity'] > 3000
    
    logger.info(f"Завершение парсинга данных клиента: {client_data.get('accountId', 'Unknown')}")
    return parsed_data

def get_commercial_addresses(report_id: int) -> List[str]:
    """
    Получает список адресов из конкретного отчета
    
    Args:
        report_id: ID отчета
    
    Returns:
        Список адресов из отчета
    """
    try:
        # Получаем все адреса из отчета
        addresses = db.query(Client.address).filter(Client.report_id == report_id).all()
        return [addr[0] for addr in addresses if addr[0]]  # Возвращаем только непустые адреса
    except Exception as e:
        print(f"Ошибка при получении адресов из отчета {report_id}: {str(e)}")
        return []

def parse_report_file(file_path_or_data: Union[str, List[Dict]]) -> List[Dict[str, Any]]:
    """
    Парсит JSON файл отчёта или список данных и возвращает список данных клиентов
    """
    logger.info(f"Начало парсинга данных")
    try:
        # Получаем данные
        if isinstance(file_path_or_data, str):
            # Если передан путь к файлу
            with open(file_path_or_data, 'r', encoding='utf-8') as f:
                report_data = json.load(f)
            logger.info(f"Файл успешно прочитан, размер данных: {len(str(report_data))} байт")
        else:
            # Если переданы данные напрямую
            report_data = file_path_or_data
            logger.info(f"Получены данные напрямую, размер данных: {len(str(report_data))} байт")
            
        # Предполагаем, что данные клиентов находятся в списке
        if isinstance(report_data, list):
            logger.info(f"Данные в формате списка, количество записей: {len(report_data)}")
            clients_data = [parse_client_data(client) for client in report_data]
        # Если данные вложены в объект
        elif isinstance(report_data, dict):
            logger.info("Данные в формате словаря, поиск списка клиентов")
            # Ищем ключ, содержащий список клиентов
            for key, value in report_data.items():
                if isinstance(value, list):
                    logger.info(f"Найден список клиентов в ключе {key}, количество записей: {len(value)}")
                    clients_data = [parse_client_data(client) for client in value]
                    break
        else:
            logger.warning(f"Неизвестный формат данных: {type(report_data)}")
            clients_data = []
            
        # Преобразуем в DataFrame для заполнения пропусков
        logger.info("Преобразование данных в DataFrame")
        df = pd.DataFrame(clients_data)
        
        # Извлекаем регион из адреса для группировки
        logger.info("Извлечение регионов из адресов для группировки")
        df['region'] = df['address'].apply(lambda x: x.split(',')[0].strip() if pd.notna(x) and ',' in x else 'Unknown')
        
        # Заполняем пропуски медианными значениями для числовых полей
        logger.info("Заполнение пропущенных значений")
        numeric_columns = ['home_area', 'season_index', 'people_count', 'rooms_count']
        for col in numeric_columns:
            if col in df.columns:
                logger.info(f"Заполнение пропусков для колонки: {col}")
                df = fill_missing_by_group(df, col, group_cols=['home_type', 'region'])
        
        # Преобразуем DataFrame обратно в список словарей
        logger.info("Преобразование DataFrame обратно в список словарей")
        result = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                if col == 'region':  # Пропускаем поле region
                    continue
                value = row[col]
                if pd.isna(value):
                    record[col] = None
                elif col in ['people_count', 'rooms_count']:
                    record[col] = int(value) if value is not None else None
                elif col in ['home_area', 'season_index']:
                    record[col] = float(value) if value is not None else None
                else:
                    record[col] = value
            result.append(record)
        
        logger.info(f"Парсинг данных завершен, обработано записей: {len(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге данных: {str(e)}", exc_info=True)
        return []

def process_report(file_path_or_data: Union[str, List[Dict]], report_id: int) -> List[Client]:
    """
    Обрабатывает отчёт и создает объекты Client
    
    Args:
        file_path_or_data: Путь к файлу или список данных
        report_id: ID отчета
    
    Returns:
        List[Client]: Список объектов Client
    """
    logger.info(f"Начало обработки отчета {report_id}")
    clients_data = parse_report_file(file_path_or_data)
    logger.info(f"Создание объектов Client для отчета {report_id}")
    clients = [
        Client(
            **{k: v for k, v in client_data.items() if k != 'region'},
            report_id=report_id
        )
        for client_data in clients_data
    ]
    logger.info(f"Обработка отчета {report_id} завершена, создано объектов: {len(clients)}")
    return clients 