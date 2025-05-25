import json
from typing import List, Dict, Any

def process_clients_data(input_file: str, output_file: str) -> None:
    # Читаем исходные данные
    with open(input_file, 'r', encoding='utf-8') as f:
        clients_data = json.load(f)
    
    # Обрабатываем данные
    processed_data = []
    for client in clients_data:
        processed_client = {
            "accountId": client["accountId"],
            "address": client["address"],
            "buildingType": client["buildingType"],
            "roomsCount": client["roomsCount"],
            "residentsCount": client.get("residentsCount"),
            "totalArea": client["totalArea"],
            "consumption": client["consumption"]
        }
        
        # Вычисляем среднее потребление за месяц
        monthly_consumptions = client["consumption"].values()
        avg_consumption = sum(monthly_consumptions) / len(monthly_consumptions)
        processed_client["is_commercial"] = avg_consumption > 3000
        
        processed_data.append(processed_client)
    
    # Сохраняем результат в JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    input_file = "./clients.json"
    output_file = "./processed_clients.json"
    process_clients_data(input_file, output_file) 