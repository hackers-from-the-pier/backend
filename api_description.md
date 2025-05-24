# API Описание для True Kilowatt

## Базовый URL
```
/api/v1
```

## Эндпоинты

### Клиенты
#### GET /client/list
Получение списка всех клиентов.

**Ответ:**
```json
[
    {
        "id": "integer",
        "name": "string",
        "email": "string",
        "phone": "string",
        "address": "string",
        "is_commercial": "boolean",
        "home_type": "string",
        "home_area": "float",
        "season_index": "float",
        "people_count": "integer",
        "rooms_count": "integer",
        "frod_state": "string",
        "frod_procentage": "float",
        "frod_yandex": "string",
        "frod_avito": "string",
        "frod_2gis": "string"
    }
]
```

### Отчеты
#### POST /report/create
Создание нового отчета.

**Ответ:**
```json
{
    "id": "integer",
    "staff_id": "integer",
    "is_ready": "boolean"
}
```

#### POST /report/{report_id}/upload
Загрузка файла в отчет.

**Параметры:**
- `report_id`: ID отчета (path parameter)
- `file`: Файл для загрузки (form-data)

**Ответ:**
```json
{
    "id": "integer",
    "is_parsed": "boolean",
    "report_id": "integer",
    "s3_url": "string"
}
```

#### POST /report/{report_id}/check
Запуск проверки отчета.

**Параметры:**
- `report_id`: ID отчета (path parameter)

**Ответ:**
```json
{
    "message": "Проверка запущена"
}
```

#### GET /report/list
Получение списка всех отчетов.

**Ответ:**
```json
[
    {
        "id": "integer",
        "staff_id": "integer",
        "is_ready": "boolean"
    }
]
```

## Модели данных

### Client
```json
{
    "id": "integer",
    "name": "string",
    "email": "string",
    "phone": "string",
    "address": "string",
    "is_commercial": "boolean",
    "home_type": "string",
    "home_area": "float",
    "season_index": "float",
    "people_count": "integer",
    "rooms_count": "integer",
    "frod_state": "string",
    "frod_procentage": "float",
    "frod_yandex": "string",
    "frod_avito": "string",
    "frod_2gis": "string"
}
```

### Report
```json
{
    "id": "integer",
    "staff_id": "integer",
    "is_ready": "boolean"
}
```

### File
```json
{
    "id": "integer",
    "is_parsed": "boolean",
    "report_id": "integer",
    "s3_url": "string"
}
```

## Коды ошибок
- 401: Неверные учетные данные
- 403: Нет доступа
- 404: Ресурс не найден
- 500: Внутренняя ошибка сервера 