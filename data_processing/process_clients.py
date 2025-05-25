import json
from typing import List, Dict, Any
from data_cleaning.parse_report import parse_report_file

def process_clients_data(input_file: str, output_file: str) -> None:
    # Читаем исходные данные
    with open(input_file, 'r', encoding='utf-8') as f:
        clients_data = json.load(f)
    
    # Обрабатываем данные через parse_report_file
    processed_data = parse_report_file(clients_data)
    
    # Добавляем поле is_commercial
    for client in processed_data:
        # Проверяем среднее потребление за месяц
        monthly_consumptions = client.get('consumption', {}).values()
        if monthly_consumptions:
            avg_consumption = sum(monthly_consumptions) / len(monthly_consumptions)
            client['is_commercial'] = avg_consumption > 3000
    
    # Сохраняем результат в JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    input_file = "./clients.json"
    output_file = "./processed_clients.json"
    process_clients_data(input_file, output_file) 