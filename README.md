# backend
Репа с кодом апишки и прочего-прочего-прочего

## Переменные окружения (.env)

### Настройки базы данных
- `DB_HOST` - хост базы данных
- `DB_PORT` - порт базы данных
- `DB_NAME` - название базы данных
- `DB_USER` - пользователь базы данных
- `DB_PASSWORD` - пароль базы данных

### Настройки API
- `API_HOST` - хост API (по умолчанию: "0.0.0.0")
- `API_PORT` - порт API (по умолчанию: "8000")
- `API_VERSION` - версия API (по умолчанию: "v1")
- `API_RELOAD` - автоматическая перезагрузка при изменении кода (по умолчанию: "True")

### Пример файла .env
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=truekilowatt
DB_USER=postgres
DB_PASSWORD=postgres

API_HOST=0.0.0.0
API_PORT=8000
API_VERSION=v1
API_RELOAD=True
```

