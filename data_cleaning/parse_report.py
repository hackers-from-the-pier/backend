import json
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from utils.models import Client

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

def fill_missing_with_median(df: pd.DataFrame, column: str, group_cols: List[str] = ['home_type']) -> pd.Series:
    """
    Заполняет пропущенные значения медианными значениями по группам
    """
    # Определяем тип данных для колонки
    if column in ['people_count', 'rooms_count']:
        dtype = 'Int64'  # nullable integer
    else:
        dtype = 'float64'
    
    # Вычисляем медианы по группам
    group_medians = df.groupby(group_cols)[column].transform('median')
    
    # Заполняем пропуски медианами с явным указанием типа
    filled = df[column].fillna(group_medians)
    
    # Преобразуем значения в нужный тип
    if dtype == 'Int64':
        # Для целочисленных полей
        filled = filled.apply(lambda x: int(x) if pd.notna(x) else None)
    else:
        # Для полей с плавающей точкой
        filled = filled.apply(lambda x: float(x) if pd.notna(x) else None)
    
    return filled

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
                    parsed_data[model_field] = float(str(value).replace(',', '.'))
                except (ValueError, TypeError):
                    parsed_data[model_field] = None
            else:
                parsed_data[model_field] = None
        else:
            parsed_data[model_field] = value
    
    # Добавляем поля, которых нет во входных данных
    additional_fields = {
        'name': None,
        'email': None,
        'phone': None,
        'season_index': None,
        'frod_state': None,
        'frod_procentage': None,
        'frod_yandex': None,
        'frod_avito': None,
        'frod_2gis': None
    }
    parsed_data.update(additional_fields)
    
    return parsed_data

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
        
        # Заполняем пропуски медианными значениями для числовых полей
        numeric_columns = ['home_area', 'season_index', 'people_count', 'rooms_count', 'frod_procentage']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = fill_missing_with_median(df, col)
        
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
            **client_data,
            report_id=report_id
        )
        for client_data in clients_data
    ] 