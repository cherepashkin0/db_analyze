import xml.etree.ElementTree as ET
from datetime import datetime

def parse_db_xml(xml_string):
    """Парсит XML ответа DB и возвращает список задержек."""
    if not xml_string:
        return []
    
    root = ET.fromstring(xml_string)
    extracted_data = []
    
    # В IRIS формате задержки лежат в атрибутах 'ct' (changed time) 
    # элементов 'ar' (arrival) или 'dp' (departure)
    for s in root.findall(".//s"):
        train_id = s.attrib.get('id', 'unknown')
        ar = s.find("ar")
        if ar is not None:
            planned_arrival = ar.attrib.get('pt') # Planned time
            changed_arrival = ar.attrib.get('ct') # Changed time
            
            if planned_arrival and changed_arrival:
                # Рассчитываем задержку
                fmt = "%y%m%d%H%M"
                dt_p = datetime.strptime(planned_arrival, fmt)
                dt_c = datetime.strptime(changed_arrival, fmt)
                delay = int((dt_c - dt_p).total_seconds() / 60)
                
                extracted_data.append({
                    'timestamp': datetime.now(),
                    'city': 'Unknown', # Мы добавим это позже по EVA_ID
                    'train_type': train_id.split()[0] if train_id else 'Train',
                    'delay_in_min': max(0, delay)
                })
    return extracted_data