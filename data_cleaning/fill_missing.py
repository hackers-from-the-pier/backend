import pandas as pd

# Заполнение пропусков средними по buildingType и region
def fill_missing_by_group(df, column, group_cols=['buildingType', 'region']):
    # Вычисляем средние по группам
    group_means = df.groupby(group_cols)[column].mean()

    # Функция для заполнения пропусков
    def fill_row(row):
        if pd.isna(row[column]):
            try:
                # Ищем среднее для комбинации buildingType и region
                return group_means.get((row['buildingType'], row['region']),
                                       # Если нет данных в группе, берем среднее по buildingType
                                       df[df['buildingType'] == row['buildingType']][column].mean() or
                                       # Если и это не работает, берем глобальное среднее
                                       df[column].mean())
            except:
                return df[column].mean()
        return row[column]

    # Применяем функцию
    df[column] = df.apply(fill_row, axis=1)
    return df