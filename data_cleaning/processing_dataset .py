import pandas as pd
from fill_missing import fill_missing_by_group

# Загрузка тестового и трэин набора
test_data = pd.read_json('dataset_test.json')
train_data = pd.read_json('dataset_train.json')

# Извлечение региона
test_data['region'] = test_data['address'].apply(lambda x: x.split(',')[0].strip() if ',' in x else 'Unknown')

# Извлечение региона из address
train_data['region'] = train_data['address'].apply(lambda x: x.split(',')[0].strip() if ',' in x else 'Unknown')

# Заполнение пропусков, используя средние из train_data
for col in ['roomsCount', 'residentsCount', 'totalArea']:
    # Вычисляем средние из обучающего набора
    group_means = train_data.groupby(['buildingType', 'region'])[col].mean()
    building_means = train_data.groupby('buildingType')[col].mean()
    global_mean = train_data[col].mean()


    def fill_test_row(row):
        if pd.isna(row[col]):
            try:
                return group_means.get((row['buildingType'], row['region']),
                                       building_means.get(row['buildingType'], global_mean))
            except:
                return global_mean
        return row[col]


    test_data[col] = test_data.apply(fill_test_row, axis=1)


# Применяем к каждому столбцу
for col in ['roomsCount', 'residentsCount', 'totalArea']:
    train_data = fill_missing_by_group(train_data, col)

# Шаг 3: Проверка результата
print("Пропуски в тестовом наборе:")
print(test_data[['roomsCount', 'residentsCount', 'totalArea']].isnull().sum())
test_data.to_csv('test_data_filled.csv', index=False)
print("Пропуски после заполнения:")
print(train_data[['roomsCount', 'residentsCount', 'totalArea']].isnull().sum())
print("\nПервые 5 строк:")
print(train_data[['buildingType', 'region', 'roomsCount', 'residentsCount', 'totalArea']].head())
print("\nОписательная статистика:")
print(train_data[['roomsCount', 'residentsCount', 'totalArea']].describe())

# Сохранение обработанных данных
train_data.to_csv('train_data_filled.csv', index=False)