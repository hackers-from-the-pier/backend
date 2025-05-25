import json
from typing import List, Dict, Any, Optional
from faker import Faker
import pandas as pd
import numpy as np
import requests
from utils.models import Client
import urllib.parse
from .fill_missing import fill_missing_by_group

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
    Генерирует URL для поиска адреса в 2GIS и проверяет наличие признаков гостиничного бизнеса
    """
    if not address:
        return None
    
    base_url = "https://2gis.ru/novorossiysk/search/"
    encoded_address = urllib.parse.quote(address)
    
    # Заголовки для имитации реального браузера
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }
    
    # Получаем HTML-код страницы
    try:
        response = requests.get(
            f"{base_url}{encoded_address}",
            headers=headers,
            timeout=10
        )
        html_content = response.text.lower()
        
        # Список ключевых фраз для проверки
        hotel_phrases = [
            'гостиница', 'отель', 'хостел', 'апартаменты',
            'сдается', 'аренда', 'проживание', 'номер',
            'почасовая', 'посуточная', 'мини-отель'
        ]
        
        # Проверяем наличие ключевых фраз
        for phrase in hotel_phrases:
            if phrase in html_content:
                return f"{base_url}{encoded_address}"
                
    except Exception as e: pass
    
    return None

def parse_client_data(client_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает данные клиента из JSON и преобразует их в формат модели Client
    с обработкой пропущенных значений и конвертацией типов
    """
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
    
    z2gis = generate_2gis_url(parsed_data.get('address'))
    
    # Добавляем поля, которых нет во входных данных
    additional_fields = {
        'name': fake.name(),  # Генерируем ФИО
        'email': fake.email(),  # Генерируем email
        'phone': fake.phone_number(),  # Генерируем телефон
        'season_index': None,
        'frod_state': "Оценивается",
        'frod_procentage': 30 if z2gis else 0,
        'frod_yandex': None,
        'frod_avito': None,
        'frod_2gis': z2gis
    }
    
    parsed_data.update(additional_fields)
    
        # Проверяем, является ли клиент коммерческим
    is_commercial = client_data.get('isCommercial', False)
    if is_commercial:
        # Для коммерческих клиентов устанавливаем frod_procentage = 0
        # и пропускаем дальнейшую обработку
        parsed_data['is_commercial'] = True
        parsed_data['frod_procentage'] = 0.0
        parsed_data['frod_state'] = "Нормальный"
    
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
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
            
        # Предполагаем, что данные клиентов находятся в списке
        if isinstance(report_data, list):
            clients_data = [parse_client_data(client) for client in report_data]
        # Если данные вложены в объект
        elif isinstance(report_data, dict):
            # Ищем ключ, содержащий список клиентов
            for key, value in report_data.items():
                if isinstance(value, list):
                    clients_data = [parse_client_data(client) for client in value]
                    break
        else:
            clients_data = []
            
        # Преобразуем в DataFrame для заполнения пропусков
        df = pd.DataFrame(clients_data)
        
        # Извлекаем регион из адреса
        df['region'] = df['address'].apply(lambda x: x.split(',')[0].strip() if pd.notna(x) and ',' in x else 'Unknown')
        
        # Заполняем пропуски медианными значениями для числовых полей
        numeric_columns = ['home_area', 'season_index', 'people_count', 'rooms_count', 'frod_procentage']
        for col in numeric_columns:
            if col in df.columns:
                df = fill_missing_by_group(df, col, group_cols=['home_type', 'region'])
        
        # Преобразуем DataFrame обратно в список словарей
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
        
        return result
        
    except Exception as e:
        print(f"Ошибка при парсинге файла {file_path}: {str(e)}")
        return []

def process_report(file_path: str, report_id: int) -> List[Client]:
    """
    Обрабатывает отчёт и создает объекты Client
    """
    clients_data = parse_report_file(file_path)
    return [
        Client(
            **{k: v for k, v in client_data.items() if k != 'region'},
            report_id=report_id
        )
        for client_data in clients_data
    ] 