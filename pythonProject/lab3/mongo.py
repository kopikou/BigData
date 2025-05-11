import pyodbc
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

# Параметры подключения к исходной реляционной БД
relational_conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-I76BBVD\SQLKOPIKOU;'
    'DATABASE=Hostel;'
    'Trusted_Connection=yes;'
)
sql_conn = pyodbc.connect(relational_conn_str)
sql_cursor = sql_conn.cursor()

# Настройки подключения к MongoDB
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'hostel_management'
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]


def migrate_hostels():
    print("Миграция хостелов...")

    # Получаем все хостелы
    sql_cursor.execute("SELECT id, city, name FROM hostel")
    hostels = sql_cursor.fetchall()

    for hostel in hostels:
        hostel_id = hostel.id

        # Получаем комнаты для этого хостела
        sql_cursor.execute(f"""
            SELECT r.id, r.type_of_gender, r.number_of_seats, r.type_comfort, 
                   ct.type as comfort_type, ct.price as comfort_price
            FROM rooms r
            LEFT JOIN comfortt ct ON r.type_comfort = ct.id
            WHERE r.hostel_id = {hostel_id}
        """)
        rooms = sql_cursor.fetchall()

        rooms_list = []
        for room in rooms:
            room_id = room.id

            # Получаем места в комнате
            sql_cursor.execute(f"SELECT id FROM seat WHERE room_id = {room_id}")
            seats = sql_cursor.fetchall()

            seats_list = []
            for seat in seats:
                # Проверяем статус места
                sql_cursor.execute(f"""
                    SELECT CASE 
                        WHEN EXISTS (SELECT 1 FROM accommodation WHERE room_id = {room_id} 
                                    AND GETDATE() BETWEEN start_date AND end_date) THEN 'occupied'
                        WHEN EXISTS (SELECT 1 FROM reservation WHERE seat_id = {seat.id} 
                                    AND GETDATE() BETWEEN start_date AND end_date) THEN 'reserved'
                        ELSE 'available'
                    END AS status
                """)
                status = sql_cursor.fetchone().status

                seats_list.append({
                    "seat_id": seat.id,
                    "status": status
                })

            rooms_list.append({
                "room_id": room_id,
                "type_of_gender": room.type_of_gender,
                "number_of_seats": room.number_of_seats,
                "comfort_type": {
                    "comfort_id": room.type_comfort,
                    "type": room.comfort_type,
                    "price": room.comfort_price
                },
                "seats": seats_list
            })

        # Получаем услуги для этого хостела с их ценами
        sql_cursor.execute(f"""
            SELECT s.id, s.name, p.price as service_price
            FROM service s
            JOIN price p ON s.price_service = p.id
            WHERE s.hostel_id = {hostel_id}
        """)
        services = sql_cursor.fetchall()

        services_list = []
        for service in services:
            services_list.append({
                "service_id": service.id,
                "name": service.name,
                "price": service.service_price
            })

        # Создаем документ хостела
        hostel_doc = {
            "hostel_id": hostel_id,
            "name": hostel.name,
            "city": hostel.city,
            "rooms": rooms_list,
            "services": services_list
        }

        db.hostel.insert_one(hostel_doc)

    print(f"Перенесено {len(hostels)} хостелов")

def migrate_employees():
    print("Миграция сотрудников...")

    sql_cursor.execute("SELECT id, full_name, date_of_birth FROM employee")
    employees = sql_cursor.fetchall()

    for employee in employees:
        employee_id = employee.id

        # Получаем хостелы, с которыми связан сотрудник (через клиентов)
        sql_cursor.execute(f"""
            SELECT DISTINCT h.id 
            FROM hostel h
            JOIN rooms r ON h.id = r.hostel_id
            JOIN accommodation a ON r.id = a.room_id
            JOIN client_registration_card c ON a.client_id = c.id
            WHERE c.employee_id = {employee_id}
        """)
        assigned_hostels = [row.id for row in sql_cursor.fetchall()]
        date_of_birth = datetime.combine(employee.date_of_birth, datetime.min.time())

        employee_doc = {
            "employee_id": employee_id,
            "full_name": employee.full_name,
            "date_of_birth": date_of_birth,
            "assigned_hostels": assigned_hostels
        }

        db.employee.insert_one(employee_doc)

    print(f"Перенесено {len(employees)} сотрудников")


def migrate_clients():
    print("Миграция клиентов...")

    sql_cursor.execute("SELECT id, full_name, phone, gender, employee_id FROM client_registration_card")
    clients = sql_cursor.fetchall()

    for client in clients:
        client_id = client.id

        # Получаем регистрации клиента
        registrations = [{
            "employee_id": client.employee_id,
            "registration_date": datetime.now()  # Здесь должна быть реальная дата регистрации
        }]

        client_doc = {
            "client_id": client_id,
            "full_name": client.full_name,
            "phone": client.phone,
            "gender": client.gender,
            "registrations": registrations
        }

        db["client"].insert_one(client_doc)
        #db.client.insert_one(client_doc)

    print(f"Перенесено {len(clients)} клиентов")

def migrate_pricing():
    print("Миграция цен...")

    # Получаем все цены и связанные с ними типы комнат
    sql_cursor.execute("""
        SELECT p.id, p.price, p.start_date, p.end_date, 
               s.hostel_id
        FROM price p
        JOIN service s ON p.id = s.price_service
    """)

    pricing_data = sql_cursor.fetchall()

    # Группируем цены по хостелам и типам комнат
    for price in pricing_data:
        start_date = datetime.combine(price.start_date, datetime.min.time())
        end_date = datetime.combine(price.end_date, datetime.min.time())

        prices_list = []
        prices_list.append({
            "price": price.price,
            "start_date": start_date,
            "end_date": end_date
        })
        pricing_map = {
            "hostel_id": price.hostel_id,
            "prices": prices_list
        }

        db.pricing.insert_one(pricing_map)

    print(f"Перенесено {len(pricing_data)} записей о ценах")

def migrate_reservations():
    print("Миграция бронирований...")

    sql_cursor.execute("""
        SELECT r.client_id, r.seat_id, r.start_date, r.end_date, 
               c.full_name, c.phone, s.room_id, rm.hostel_id, h.name, rm.type_of_gender
        FROM reservation r
        JOIN client_registration_card c ON r.client_id = c.id
        JOIN seat s ON r.seat_id = s.id
        JOIN rooms rm ON s.room_id = rm.id
        JOIN hostel h ON rm.hostel_id = h.id
    """)
    reservations = sql_cursor.fetchall()

    for res in reservations:
        # Получаем информацию о комфорте комнаты
        sql_cursor.execute(f"""
            SELECT ct.type 
            FROM rooms r
            JOIN comfortt ct ON r.type_comfort = ct.id
            WHERE r.id = {res.room_id}
        """)
        #comfort_type = sql_cursor.fetchone().type if sql_cursor.fetchone() else "Standard"
        comfort_row = sql_cursor.fetchone()
        comfort_type = comfort_row.type if comfort_row else "Standard"

        # Преобразуем даты в datetime
        start_date = datetime.combine(res.start_date, datetime.min.time())
        end_date = datetime.combine(res.end_date, datetime.min.time())

        reservation_doc = {
            "reservation_id": ObjectId(),  # Генерируем новый ID
            "client_id": res.client_id,
            "client_info": {
                "full_name": res.full_name,
                "phone": res.phone
            },
            "hostel_info": {
                "hostel_id": res.hostel_id,
                "name": res.name
            },
            "room_info": {
                "room_id": res.room_id,
                "type_of_gender": res.type_of_gender,
                "comfort_type": comfort_type
            },
            "seat_id": res.seat_id,
            "dates": {
                "start_date": start_date,
                "end_date": end_date
            },
            "status": "active"
        }

        db.reservation.insert_one(reservation_doc)

    print(f"Перенесено {len(reservations)} бронирований")

def migrate_services_for_accommodations():
    print("Миграция услуг для проживаний...")

    # Для каждого проживания получаем услуги с актуальными ценами
    sql_cursor.execute("""
        SELECT sa.accommodation_id, sa.service_id, sa.cnt, 
               s.name, p.price as current_price
        FROM service_accommodation sa
        JOIN service s ON sa.service_id = s.id
        JOIN price p ON s.price_service = p.id
        WHERE GETDATE() BETWEEN p.start_date AND p.end_date
    """)

    services = sql_cursor.fetchall()

    # Группируем услуги по accommodation_id
    services_map = {}
    for service in services:
        if service.accommodation_id not in services_map:
            services_map[service.accommodation_id] = []

        services_map[service.accommodation_id].append({
            "service_id": service.service_id,
            "name": service.name,
            "unit_price": service.current_price,
            "quantity": service.cnt
        })

    return services_map


def migrate_accommodations():
    print("Миграция проживаний...")

    # Сначала получаем все услуги для проживаний
    services_map = migrate_services_for_accommodations()

    # Теперь получаем сами проживания
    sql_cursor.execute("""
        SELECT a.accommodation_id, a.client_id, a.room_id, a.start_date, a.end_date, 
               c.full_name, c.phone, r.hostel_id, h.name, h.city, 
               r.type_comfort, ct.type as comfort_type, ct.price as comfort_price
        FROM accommodation a
        JOIN client_registration_card c ON a.client_id = c.id
        JOIN rooms r ON a.room_id = r.id
        JOIN hostel h ON r.hostel_id = h.id
        JOIN comfortt ct ON r.type_comfort = ct.id
    """)

    accommodations = sql_cursor.fetchall()

    for acc in accommodations:
        # Получаем услуги для этого проживания
        services = services_map.get(acc.accommodation_id, [])

        # Рассчитываем общую стоимость
        total_services_cost = sum(s['unit_price'] * s['quantity'] for s in services)

        # Получаем базовую цену
        base_price = acc.comfort_price  # Используем цену из comfortt
        # Преобразуем даты в datetime
        start_date = datetime.combine(acc.start_date, datetime.min.time())
        end_date = datetime.combine(acc.end_date, datetime.min.time())

        accommodation_doc = {
            "accommodation_id": acc.accommodation_id,
            "client_id": acc.client_id,
            "hostel_info": {
                "hostel_id": acc.hostel_id,
                "name": acc.name,
                "city": acc.city
            },
            "room_info": {
                "room_id": acc.room_id,
                "comfort_type": {
                    "type": acc.comfort_type,
                    "price": acc.comfort_price
                }
            },
            "dates": {
                "start_date": start_date,
                "end_date": end_date
            },
            "pricing": {
                "base_price": base_price,
                "services": services,
                "total_cost": base_price + total_services_cost
            }
        }

        db.accommodation.insert_one(accommodation_doc)

    print(f"Перенесено {len(accommodations)} проживаний")

def clear_collections():
    """Очистка коллекций перед миграцией"""
    collections_to_clear = ['hostel', 'client', 'employee', 'pricing', 'reservation', 'accommodation']
    for collection in collections_to_clear:
        if collection in db.list_collection_names():
            db[collection].drop()  # Правильный способ удаления коллекции
            print(f"Коллекция {collection} очищена")
        else:
            print(f"Коллекция {collection} не существует, пропускаем")

def main():
    # Очищаем коллекции перед миграцией
    clear_collections()

    # Выполняем миграцию
    migrate_hostels()
    migrate_employees()
    migrate_clients()
    migrate_pricing()
    migrate_reservations()
    migrate_accommodations()

    print("Миграция завершена успешно!")


if __name__ == "__main__":
    main()