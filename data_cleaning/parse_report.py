import json
from typing import List, Dict, Any, Optional
from faker import Faker
import pandas as pd
import numpy as np
import requests
from utils.models import Client
import urllib.parse
from .fill_missing import fill_missing_by_group
import logging
from urllib.parse import quote

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Устанавливаем опцию для будущего поведения pandas
pd.set_option('future.no_silent_downcasting', True)

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
    Генерирует URL для поиска адреса в 2GIS
    """
    if not address:
        return None
        
    logger.info(f"Генерация 2GIS URL для адреса: {address}")
    
    # Кодируем адрес для URL
    encoded_address = quote(address)
    url = f"https://2gis.ru/novorossiysk/search/{encoded_address}"
    
    try:
        logger.info(f"Отправка запроса к 2GIS для адреса: {address}")
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        # Проверяем статус ответа
        if response.status_code == 200:
            # Декодируем контент с правильной кодировкой
            content = response.content.decode('utf-8')
            
            logger.info(content)
            
            # Проверяем наличие ключевых фраз
            hotel_phrases = [
                'гостиница', 'отель', 'хостел', 'апартаменты',
                'сдается', 'аренда', 'проживание', 'номер',
                'почасовая', 'посуточная', 'мини-отель'
            ]
            
            content_lower = content.lower()
            for phrase in hotel_phrases:
                if phrase in content_lower:
                    return url
                    
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
        'isCommercial': 'is_commercial',
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
        if json_field == 'isCommercial':
            parsed_data[model_field] = bool(value) if value is not None else False
        elif json_field in ['roomsCount', 'residentsCount']:
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
    
    # Расчет метрик потребления электроэнергии
    consumption = client_data.get('consumption', {})
    if consumption:
        logger.info(f"Расчет метрик потребления для клиента {client_data.get('accountId', 'Unknown')}")
        # Преобразуем значения в числа и фильтруем None
        monthly_values = [float(v) for v in consumption.values() if v is not None]
        
        if monthly_values:
            # Суммарное потребление за год
            parsed_data['summary_electricity'] = round(sum(monthly_values), 2)
            
            # Среднее потребление в месяц
            parsed_data['avg_monthly_electricity'] = round(sum(monthly_values) / len(monthly_values), 2)
            
            # Максимальное потребление за месяц
            parsed_data['max_monthly_electricity'] = round(max(monthly_values), 2)
            
            # Минимальное потребление за месяц
            parsed_data['min_monthly_electricity'] = round(min(monthly_values), 2)
            
            # Потребление на 1 м²
            if parsed_data.get('home_area'):
                parsed_data['electricity_per_sqm'] = round(parsed_data['summary_electricity'] / parsed_data['home_area'], 2)
            
            # Потребление на 1 человека
            if parsed_data.get('people_count'):
                parsed_data['electricity_per_person'] = round(parsed_data['summary_electricity'] / parsed_data['people_count'], 2)
    
    # Инициализируем Faker с русской локалью
    fake = Faker('ru_RU')
    
    # Добавляем поля, которых нет во входных данных
    additional_fields = {
        'name': fake.name(),  # Генерируем ФИО
        'email': fake.email(),  # Генерируем email
        'phone': fake.phone_number(),  # Генерируем телефон
        'season_index': None,
        'frod_state': "Оценивается",
        'frod_procentage': 0,
        'frod_yandex': None,
        'frod_avito': None,
        'frod_2gis': None
    }
    
    parsed_data.update(additional_fields)
    
    # Проверяем, является ли клиент коммерческим
    is_commercial = client_data.get('isCommercial', False)
    if is_commercial:
        logger.info(f"Клиент {client_data.get('accountId', 'Unknown')} определен как коммерческий")
        # Для коммерческих клиентов устанавливаем frod_procentage = 0
        # и пропускаем дальнейшую обработку
        parsed_data['is_commercial'] = True
        parsed_data['frod_procentage'] = 0.0
        parsed_data['frod_state'] = "Нормальный"
    
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

def parse_report_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Парсит JSON файл отчёта и возвращает список данных клиентов
    """
    logger.info(f"Начало парсинга файла: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
            logger.info(f"Файл успешно прочитан, размер данных: {len(str(report_data))} байт")
            
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
            logger.warning(f"Неизвестный формат данных в файле: {type(report_data)}")
            clients_data = []
            
        # Преобразуем в DataFrame для заполнения пропусков
        logger.info("Преобразование данных в DataFrame")
        df = pd.DataFrame(clients_data)
        
        # Извлекаем регион из адреса
        logger.info("Извлечение регионов из адресов")
        df['region'] = df['address'].apply(lambda x: x.split(',')[0].strip() if pd.notna(x) and ',' in x else 'Unknown')
        
        # Заполняем пропуски медианными значениями для числовых полей
        logger.info("Заполнение пропущенных значений")
        numeric_columns = ['home_area', 'season_index', 'people_count', 'rooms_count', 'frod_procentage']
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
                value = row[col]
                if pd.isna(value):
                    record[col] = None
                elif col in ['people_count', 'rooms_count']:
                    record[col] = int(value) if value is not None else None
                elif col in ['home_area', 'season_index', 'frod_procentage']:
                    record[col] = float(value) if value is not None else None
                else:
                    record[col] = value
            result.append(record)
        
        logger.info(f"Парсинг файла завершен, обработано записей: {len(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге файла {file_path}: {str(e)}", exc_info=True)
        return []

def process_report(file_path: str, report_id: int) -> List[Client]:
    """
    Обрабатывает отчёт и создает объекты Client
    """
    logger.info(f"Начало обработки отчета {report_id} из файла {file_path}")
    clients_data = parse_report_file(file_path)
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