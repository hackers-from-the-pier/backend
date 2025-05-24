import json
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from utils.models import Client

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
    # Вычисляем медианы по группам
    group_medians = df.groupby(group_cols)[column].transform('median')
    # Заполняем пропуски медианами
    filled = df[column].fillna(group_medians)
    # Заменяем NaN на None
    return filled.apply(lambda x: None if pd.isna(x) else x)

def parse_client_data(client_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает данные клиента из JSON и преобразует их в формат модели Client
    с обработкой пропущенных значений и конвертацией типов
    """
    # Определяем типы полей из модели Client
    field_types = {
        'name': str,
        'email': str,
        'phone': str,
        'address': str,
        'is_commercial': bool,
        'home_type': str,
        'home_area': float,
        'season_index': float,
        'people_count': int,
        'rooms_count': int,
        'frod_state': str,
        'frod_procentage': float,
        'frod_yandex': str,
        'frod_avito': str,
        'frod_2gis': str
    }
    
    parsed_data = {}
    
    for field, field_type in field_types.items():
        # Получаем значение из входных данных
        value = client_data.get(field)
        
        # Конвертируем значение в нужный тип
        converted_value = convert_value(value, field_type)
        
        # Для числовых полей проверяем на отрицательные значения
        if field_type in (int, float) and converted_value is not None:
            if converted_value < 0:
                converted_value = None
        
        # Для строковых полей проверяем на пустые строки
        if field_type == str and converted_value == "":
            converted_value = None
            
        parsed_data[field] = converted_value
    
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
        
        # Преобразуем DataFrame обратно в список словарей, заменяя NaN на None
        return df.apply(lambda x: x.apply(lambda y: None if pd.isna(y) else y)).to_dict('records')
        
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