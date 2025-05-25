import json
from typing import List, Dict, Any, Union

def process_clients_data(input_data: Union[str, List[Dict]], output_file: str = None) -> Union[None, List[Dict]]:
    # Получаем данные
    if isinstance(input_data, str):
        # Если передан путь к файлу
        with open(input_data, 'r', encoding='utf-8') as f:
            clients_data = json.load(f)
    else:
        # Если переданы данные напрямую
        clients_data = input_data
    
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
    
    # Если указан файл для сохранения, сохраняем результат
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)
        return None
    
    # Иначе возвращаем обработанные данные
    return processed_data

if __name__ == "__main__":
    # Пример использования с файлом
    input_file = "./clients.json"
    output_file = "./processed_clients.json"
    process_clients_data(input_file, output_file)
    
    # Пример использования с данными напрямую
    # clients_data = [...] # список с данными клиентов
    # processed_data = process_clients_data(clients_data) 