# iris_parser.py

import xml.etree.ElementTree as ET
from datetime import datetime

def parse_db_xml(xml_string, city_name):
    if not xml_string:
        return []
    
    try:
        root = ET.fromstring(xml_string)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []

    records = []
    
    for s in root.findall(".//s"):
        # 1. ID и Тип
        raw_id = s.attrib.get('id', 'unknown')
        tl = s.find("tl")
        train_type = tl.attrib.get('c', 'Train') if tl is not None else "Train"
        train_number = tl.attrib.get('n', '') if tl is not None else ""
        
        full_train_id = f"{train_type} {train_number}".strip() or raw_id

        # 2. Поиск времени и статуса отмены
        # ar = arrival (прибытие), dp = departure (отправление)
        ar = s.find("ar")
        dp = s.find("dp")
        
        # Предпочитаем отправление (dp), если это не конечная станция
        target_node = dp if dp is not None else ar
        
        # По умолчанию поезд не отменен
        is_cancelled = 0
        
        # В IRIS 'cs' или 'cp' со значением 'c' означает отмену
        if target_node is not None:
            # Проверяем атрибуты отмены (cs - cancelled stop, cp - cancelled platform)
            if target_node.attrib.get('cs') == 'c' or target_node.attrib.get('cp') == 'c':
                is_cancelled = 1
            
            # Также статус может быть в родительском теге <s>
            if s.attrib.get('bs') == 'c': # bs - busy status (иногда используется для отмен)
                 is_cancelled = 1

            pt_str = target_node.attrib.get('pt')
            ct_str = target_node.attrib.get('ct')
            
            # Если поезд отменен, ct (changed time) часто нет. Берем плановое.
            if not ct_str:
                ct_str = pt_str

            if pt_str and ct_str:
                try:
                    fmt = "%y%m%d%H%M"
                    dt_planned = datetime.strptime(pt_str, fmt)
                    dt_actual = datetime.strptime(ct_str, fmt)
                    
                    delay = int((dt_actual - dt_planned).total_seconds() / 60)
                    
                    # Если поезд отменен, задержка не имеет смысла (или можно ставить 0)
                    if is_cancelled:
                        delay = 0

                    records.append([
                        datetime.now(),
                        city_name,
                        train_type,
                        full_train_id,
                        dt_planned,
                        dt_actual,
                        max(0, delay),
                        is_cancelled  # <--- НОВОЕ ПОЛЕ
                    ])
                except (ValueError, TypeError):
                    continue
    return records