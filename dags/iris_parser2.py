import xml.etree.ElementTree as ET
from datetime import datetime

def parse_db_xml(xml_string, city_name):
    """Парсит XML (IRIS format) и возвращает данные для ClickHouse."""
    if not xml_string:
        return []
    
    try:
        root = ET.fromstring(xml_string)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []

    records = []
    # В формате fchg (changes) нас интересуют узлы <s> (stations)
    for s in root.findall(".//s"):
        train_id = s.attrib.get('id', 'unknown')
        # Ищем изменения в прибытии (ar) или отправлении (dp)
        ar = s.find("ar")
        dp = s.find("dp")
        
        # Берем актуальное время изменения (ct - changed time)
        # Формат времени в DB: YYMMDDHHMM (например 2601211330)
        changed_time_str = None
        planned_time_str = None
        
        if ar is not None and 'ct' in ar.attrib and 'pt' in ar.attrib:
            changed_time_str = ar.attrib['ct']
            planned_time_str = ar.attrib['pt']
        elif dp is not None and 'ct' in dp.attrib and 'pt' in dp.attrib:
            changed_time_str = dp.attrib['ct']
            planned_time_str = dp.attrib['pt']

        if changed_time_str and planned_time_str:
            fmt = "%y%m%d%H%M"
            try:
                dt_planned = datetime.strptime(planned_time_str, fmt)
                dt_actual = datetime.strptime(changed_time_str, fmt)
                delay = int((dt_actual - dt_planned).total_seconds() / 60)
                
                # Ограничимся положительными задержками для статистики
                delay = max(0, delay)
                
                records.append([
                    datetime.now(), # Текущий момент (timestamp вставки)
                    city_name,
                    train_id.split()[0], # Например: ICE, RE, S
                    delay
                ])
            except ValueError:
                continue
    return records