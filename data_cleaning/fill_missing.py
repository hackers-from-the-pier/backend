import pandas as pd
from typing import List

# Заполнение пропусков средними по buildingType и region
def fill_missing_by_group(df: pd.DataFrame, target_col: str, group_cols: List[str]) -> pd.DataFrame:
    """
    Заполняет пропущенные значения медианными значениями с учетом группы и нормализации по площади и количеству людей.
    
    Args:
        df: DataFrame с данными
        target_col: Название столбца, который нужно заполнить
        group_cols: Список столбцов для группировки
    
    Returns:
        DataFrame с заполненными пропущенными значениями
    """
    # Создаем копию DataFrame
    df = df.copy()
    
    # Определяем, является ли целевой столбец метрикой потребления
    is_consumption_metric = target_col in ['summary_electricity', 'avg_monthly_electricity', 
                                         'max_monthly_electricity', 'min_monthly_electricity',
                                         'electricity_per_sqm', 'electricity_per_person']
    
    # Для метрик потребления сначала нормализуем значения
    if is_consumption_metric:
        # Создаем временные столбцы для нормализованных значений
        if target_col in ['electricity_per_sqm', 'summary_electricity', 'avg_monthly_electricity', 
                         'max_monthly_electricity', 'min_monthly_electricity']:
            # Нормализуем по площади
            df['temp_normalized'] = df[target_col] / df['home_area']
        elif target_col == 'electricity_per_person':
            # Нормализуем по количеству людей
            df['temp_normalized'] = df[target_col] / df['people_count']
        
        # Заполняем пропуски в нормализованных значениях
        for group in df.groupby(group_cols):
            group_idx = group[1].index
            group_data = group[1]
            
            # Находим медиану нормализованных значений для группы
            median_value = group_data['temp_normalized'].median()
            
            # Заполняем пропуски медианным значением
            df.loc[group_idx, 'temp_normalized'] = df.loc[group_idx, 'temp_normalized'].fillna(median_value)
        
        # Возвращаем значения к исходному масштабу
        if target_col in ['electricity_per_sqm', 'summary_electricity', 'avg_monthly_electricity', 
                         'max_monthly_electricity', 'min_monthly_electricity']:
            df[target_col] = df['temp_normalized'] * df['home_area']
        elif target_col == 'electricity_per_person':
            df[target_col] = df['temp_normalized'] * df['people_count']
        
        # Удаляем временный столбец
        df = df.drop('temp_normalized', axis=1)
    else:
        # Для остальных метрик используем стандартное заполнение
        for group in df.groupby(group_cols):
            group_idx = group[1].index
            group_data = group[1]
            
            # Находим медиану для группы
            median_value = group_data[target_col].median()
            
            # Заполняем пропуски медианным значением
            df.loc[group_idx, target_col] = df.loc[group_idx, target_col].fillna(median_value)
    
    # Округляем все числовые столбцы до 2 знаков после запятой
    numeric_columns = df.select_dtypes(include=['float64']).columns
    for col in numeric_columns:
        df[col] = df[col].round(2)
    
    return df