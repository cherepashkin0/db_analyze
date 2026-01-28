# iris_parser.py
"""
Парсер для Deutsche Bahn Timetables API (IRIS).

Поддерживает:
- /plan/{evaNo}/{YYMMDD}/{HH} - запланированное расписание
- /fchg/{evaNo} - изменения (для будущего использования)

Формат времени в API: YYMMddHHmm (например, 2501281430 = 28.01.2025 14:30)
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional


def parse_iris_timestamp(ts: str) -> Optional[datetime]:
    """
    Парсит timestamp в формате IRIS API: YYMMddHHmm
    Например: 2501281430 -> 2025-01-28 14:30:00
    """
    if not ts or len(ts) != 10:
        return None
    try:
        return datetime.strptime(ts, "%y%m%d%H%M")
    except ValueError:
        return None


def parse_plan_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Парсит XML ответ от /plan endpoint.
    
    Возвращает список кортежей для вставки в ClickHouse:
    (timestamp, city, train_type, train_id, planned_departure, actual_departure, 
     delay_in_min, is_cancelled, origin, destination)
    
    Для /plan endpoint:
    - actual_departure = planned_departure (изменений нет)
    - delay_in_min = 0
    - is_cancelled = 0
    """
    results = []
    now = datetime.now()
    
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")
        return results
    
    # Получаем имя станции из XML если есть
    station_name = root.get('station', city)
    
    # Итерируем по всем элементам <s> (stops/trains)
    for stop in root.findall('.//s'):
        try:
            # Внутренний ID (сохраняем для уникальности)
            internal_id = stop.get('id', '')
            
            # Trip Label (tl) содержит информацию о типе поезда
            tl = stop.find('tl')
            if tl is not None:
                train_type = tl.get('c', '')  # category: ICE, IC, RE, S, etc.
                train_number = tl.get('n', '')  # number: 101, 9, 1, etc.
                # Собираем читаемый ID: "ICE 101", "RE 9", "S 1"
                if train_type and train_number:
                    train_id = f"{train_type} {train_number}"
                else:
                    train_id = internal_id  # fallback на внутренний ID
            else:
                train_type = ''
                train_id = internal_id
            
            # Arrival (ar) - прибытие
            ar = stop.find('ar')
            arrival_time = None
            origin = ''
            if ar is not None:
                pt = ar.get('pt', '')  # planned time
                arrival_time = parse_iris_timestamp(pt)
                # ppth = planned path - откуда поезд пришёл (станции ДО текущей)
                # Первая станция в списке - это начальная точка маршрута (origin)
                ppth = ar.get('ppth', '')
                if ppth:
                    origin = ppth.split('|')[0]
            
            # Departure (dp) - отправление
            dp = stop.find('dp')
            departure_time = None
            destination = ''
            if dp is not None:
                pt = dp.get('pt', '')  # planned time
                departure_time = parse_iris_timestamp(pt)
                # ppth = planned path - куда поезд едет (станции ПОСЛЕ текущей)
                # Последняя станция в списке - это конечная точка маршрута (destination)
                ppth = dp.get('ppth', '')
                if ppth:
                    destination = ppth.split('|')[-1]
            
            # Если origin пустой, но есть destination - поезд начинает маршрут здесь
            # В этом случае origin = текущая станция
            if not origin and destination:
                origin = station_name
            
            # Если destination пустой, но есть origin - поезд заканчивает маршрут здесь
            # В этом случае destination = текущая станция
            if not destination and origin:
                destination = station_name
            
            # Используем departure_time как основное время (или arrival если нет departure)
            planned_time = departure_time or arrival_time
            
            if not planned_time:
                continue  # Пропускаем записи без времени
            
            # Для /plan endpoint нет изменений, поэтому:
            actual_time = planned_time  # actual = planned
            delay_in_min = 0
            is_cancelled = 0
            
            results.append((
                now,                    # timestamp (когда загрузили)
                station_name,           # city
                train_type,             # train_type
                train_id,               # train_id (теперь "ICE 101", "RE 9", etc.)
                planned_time,           # planned_departure
                actual_time,            # actual_departure
                delay_in_min,           # delay_in_min
                is_cancelled,           # is_cancelled
                origin,                 # origin
                destination,            # destination
            ))
            
        except Exception as e:
            print(f"Error parsing stop: {e}")
            continue
    
    return results


def parse_fchg_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Парсит XML ответ от /fchg endpoint (full changes).
    
    В отличие от /plan, здесь есть changed time (ct) и changed status (cs).
    
    Возвращает список кортежей с реальными задержками.
    """
    results = []
    now = datetime.now()
    
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")
        return results
    
    station_name = root.get('station', city)
    
    for stop in root.findall('.//s'):
        try:
            # Внутренний ID (сохраняем для уникальности)
            internal_id = stop.get('id', '')
            
            tl = stop.find('tl')
            if tl is not None:
                train_type = tl.get('c', '')
                train_number = tl.get('n', '')
                if train_type and train_number:
                    train_id = f"{train_type} {train_number}"
                else:
                    train_id = internal_id
            else:
                train_type = ''
                train_id = internal_id
            
            # Arrival
            ar = stop.find('ar')
            arrival_planned = None
            arrival_changed = None
            origin = ''
            ar_cancelled = False
            
            if ar is not None:
                pt = ar.get('pt', '')
                ct = ar.get('ct', '')  # changed time
                cs = ar.get('cs', '')  # changed status: 'c' = cancelled
                
                arrival_planned = parse_iris_timestamp(pt)
                arrival_changed = parse_iris_timestamp(ct) if ct else arrival_planned
                ar_cancelled = (cs == 'c')
                
                ppth = ar.get('ppth', '')
                if ppth:
                    origin = ppth.split('|')[0]
            
            # Departure
            dp = stop.find('dp')
            departure_planned = None
            departure_changed = None
            destination = ''
            dp_cancelled = False
            
            if dp is not None:
                pt = dp.get('pt', '')
                ct = dp.get('ct', '')
                cs = dp.get('cs', '')
                
                departure_planned = parse_iris_timestamp(pt)
                departure_changed = parse_iris_timestamp(ct) if ct else departure_planned
                dp_cancelled = (cs == 'c')
                
                ppth = dp.get('ppth', '')
                if ppth:
                    destination = ppth.split('|')[-1]
            
            # Если origin пустой - поезд начинает маршрут здесь
            if not origin and destination:
                origin = station_name
            
            # Если destination пустой - поезд заканчивает маршрут здесь
            if not destination and origin:
                destination = station_name
            
            # Выбираем основное время
            planned_time = departure_planned or arrival_planned
            actual_time = departure_changed or arrival_changed
            
            if not planned_time:
                continue
            
            # Вычисляем задержку
            if actual_time and planned_time:
                delay_in_min = int((actual_time - planned_time).total_seconds() / 60)
            else:
                delay_in_min = 0
            
            is_cancelled = 1 if (ar_cancelled or dp_cancelled) else 0
            
            results.append((
                now,
                station_name,
                train_type,
                train_id,
                planned_time,
                actual_time or planned_time,
                delay_in_min,
                is_cancelled,
                origin,
                destination,
            ))
            
        except Exception as e:
            print(f"Error parsing stop: {e}")
            continue
    
    return results


# Для обратной совместимости со старым кодом
def parse_db_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Обёртка для обратной совместимости.
    Пытается определить тип ответа и вызвать нужный парсер.
    """
    # Пробуем как fchg (там обычно есть changed attributes)
    if 'ct="' in xml_data or 'cs="' in xml_data:
        return parse_fchg_xml(xml_data, city)
    else:
        return parse_plan_xml(xml_data, city)