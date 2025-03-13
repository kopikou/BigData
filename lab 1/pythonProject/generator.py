import pyodbc
import random
from faker import Faker
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Настройки подключения к базе данных
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-I76BBVD\SQLKOPIKOU;'
    'DATABASE=Hostel;'
    'Trusted_Connection=yes;'
)

fake = Faker()

# Функция для генерации сотрудников
def generate_employees(num_employees):
    employees = []
    for _ in range(num_employees):
        id = fake.unique.random_int(min=1, max=10**10)
        full_name = fake.name()
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=65)
        employees.append((id, full_name, date_of_birth))
    return employees
# Функция для получения существующих сотрудников
def get_existing_employees():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM employee")

    employee_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return employee_ids

# Функция для генерации клиентов
def generate_clients(num_clients, employees):
    clients = []
    for _ in range(num_clients):
        id = fake.unique.random_int(min=1, max=10**10)
        full_name = fake.name()
        phone = fake.phone_number()
        gender = random.choice(["Male", "Female"])
        employee_id = random.choice(employees)
        clients.append((id, full_name, phone, gender, employee_id))
    return clients
# Функция для получения существующих клиентов
def get_existing_clients():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("SELECT id, gender FROM client_registration_card")
    #cursor.execute("SELECT * FROM client_registration_card ORDER BY id OFFSET 4100 ROWS FETCH NEXT 100 ROWS ONLY;")

    client_ids = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    return client_ids

# Функция для генерации хостелов
def generate_hostels(num_hostels):
    hostels = []
    for _ in range(num_hostels):
        id = fake.unique.random_int(min=1, max=100)
        city = fake.city()
        name = fake.company()
        hostels.append((id,city,name))
    return hostels
# Функция для получения существующих хостелов
def get_existing_hostels():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM hostel")

    hostel_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return hostel_ids

# Функция для получения существующих типов комфорта
def get_existing_comforts():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM comfortt")

    comfort_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return comfort_ids

# Функция для генерации комнат
def generate_rooms(num_rooms, comforts, hostels):
    rooms = []
    for _ in range(num_rooms):
        id = fake.unique.random_int(min=1, max=1000)
        type_of_gender = random.choice(["Male", "Female", "Mixed"])
        number_of_seats = random.randint(1, 8)
        type_comfort = random.choice(comforts)
        hostel_id = random.choice(hostels)
        rooms.append((id, type_of_gender, number_of_seats, type_comfort, hostel_id))
    return rooms
# Функция для получения существующих комнат
def get_existing_rooms():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id, number_of_seats, type_of_gender FROM rooms")

    #rooms_ids = [row[0] for row in cursor.fetchall()]
    rooms_ids = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return rooms_ids

def get_seats():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM seat")

    seats_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return seats_ids
# Функция для генерации мест
def generate_seats(rooms):
    seats = []
    #for _ in range(num_seats):
    for room_id, (capacity, type_of_gender) in rooms.items():
        for _ in range(capacity):
            id = fake.unique.random_int(min=1, max=1000)
            #room_id = random.choice(rooms)
            seats.append((id, room_id))
    return seats
# Функция для получения существующих мест
def get_existing_seats(client_gender, rooms):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Получаем id комнат, соответствующих полу клиента
    available_rooms = []

    for room_id, (num_seats, room_type) in rooms.items():
        if room_type == 'Mixed' or (room_type == 'Male' and client_gender == 'Male') or (
                room_type == 'Female' and client_gender == 'Female'):
            available_rooms.append(room_id)
            #print(available_rooms)

    if not available_rooms:
        return []  # Если нет доступных комнат, возвращаем пустой список

    # Получаем все места из доступных комнат
    seat_ids = []
    for room_id in available_rooms:
        cursor.execute("SELECT id FROM seat WHERE room_id = ?",
                       room_id)
        seat_ids.extend([row[0] for row in cursor.fetchall()])

    cursor.close()
    conn.close()

    return seat_ids

# Функция для генерации броней
def generate_reservations(num_reservations, clients, rooms):
    reservations = []

    for _ in range(num_reservations):
    #for client in clients:
        client_id, client_gender = random.choice(list(clients.items()))  # Получаем клиента и его пол
        #client_id, client_gender = client, clients.get(client)

        # Получаем доступные места для данного клиента
        seats = get_existing_seats(client_gender, rooms)

        if not seats:
            continue  # Если нет доступных мест, пропускаем итерацию
        #for seat in seats:
        seat_id = random.choice(seats)  # Выбираем случайное доступное место
        #seat_id = seat
        start_date = fake.date_between(start_date='today', end_date='+30d')
        end_date = fake.date_between(start_date=fake.date_time_between(start_date=start_date, end_date=start_date) + timedelta(days=1), end_date=fake.date_time_between(start_date=start_date, end_date=start_date) + timedelta(days=90))

        reservations.append((client_id, seat_id, start_date, end_date))

    return reservations

# Функция для генерации цен
def generate_prices(num_prices):
    prices = []
    for _ in range(num_prices):
        id = random.randint(1, 1000)#fake.unique.random_int(min=1, max=1000)
        price = random.randint(500, 5000)
        start_date = fake.date_between(start_date='-1y', end_date='today')
        end_date = fake.date_between(start_date='today', end_date='+1y')
        prices.append((id, price, start_date, end_date))
    return prices
# Функция для получения существующих цен
def get_existing_prices():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM price")

    price_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return price_ids

# Функция для генерации услуг
def generate_services(num_services, hostels, prices):
    services = []
    service_names = [
        "Wi-Fi", "Breakfast", "Dinner", "Transfer service", "Room cleaning",
        "Laundry", "Fitness room", "Spa treatments", "Bike rental",
        "Excursions", "Bar", "Swimming pool", "Parking", "Conference room",
        "Cable TV", "Coffee bar"
    ]
    for _ in range(num_services):
        id = fake.unique.random_int(min=1, max=300)
        name = random.choice(service_names)
        price_service = random.choice(prices)
        hostel_id = random.choice(hostels)
        services.append((id, name, price_service, hostel_id))
    return services
# Функция для получения существующих услуг
def get_existing_services():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM service")

    #service_ids = [row[0] for row in cursor.fetchall()]
    service_ids = cursor.fetchall()

    cursor.close()
    conn.close()

    return service_ids
def get_services():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM service")

    service_ids = [row[0] for row in cursor.fetchall()]
    #service_ids = cursor.fetchall()

    cursor.close()
    conn.close()

    return service_ids

# Функция для генерации проживаний
def generate_accommodations(num_accommodations, clients, rooms):
    accommodations = []
    for _ in range(num_accommodations):
        accommodation_id = fake.unique.random_int(min=1, max=10**10)
        client_id, client_gender = random.choice(list(clients.items()))  # Получаем клиента и его пол

        available_rooms = []
        for room_id, (num_seats, room_type) in rooms.items():
            if room_type == 'Mixed' or (room_type == 'Male' and client_gender == 'Male') or (
                    room_type == 'Female' and client_gender == 'Female'):
                available_rooms.append(room_id)
        if not available_rooms:
            return []

        room_id = random.choice(available_rooms)
        start_date = fake.date_between(start_date='-30y', end_date='today')
        end_date = fake.date_between(start_date=fake.date_time_between(start_date=start_date, end_date=start_date) + timedelta(days=1), end_date=fake.date_time_between(start_date=start_date, end_date=start_date) + timedelta(days=90))#(start_date=start_date+1, end_date='+30d')
        accommodations.append((accommodation_id, client_id, room_id, start_date, end_date))
    return accommodations
# Функция для получения существующих проживаний
def get_existing_accommodations():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accommodation")

    #acc_ids = [row[0] for row in cursor.fetchall()]
    acc_ids = cursor.fetchall()

    cursor.close()
    conn.close()

    return acc_ids
def get_accommodations():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT accommodation_id FROM accommodation")

    acc_ids = [row[0] for row in cursor.fetchall()]
    #acc_ids = cursor.fetchall()

    cursor.close()
    conn.close()

    return acc_ids

# Функция для генерации услуг в проживании
def generate_service_accommodations(num_service_accommodations, services, accommodations):
    service_accommodations = []
    # Создаем словарь для быстрого доступа к услугам по хостелам
    services_by_hostel = {}
    for service in services:
        service_id, name, price_service, hostel_id = service
        if hostel_id not in services_by_hostel:
            services_by_hostel[hostel_id] = []
        services_by_hostel[hostel_id].append(service)

    for _ in range(num_service_accommodations):
    #for accommodation in accommodations:
        accommodation_id = random.choice(accommodations)
        #accommodation_id = accommodation
        # Получаем id комнаты из информации о проживании
        room_id = accommodation_id[2]  # Предполагаем, что третий элемент - это id комнаты
        # Получаем id хостела по комнате
        hostel_id = get_hostel_id_by_room(room_id)

        # Выбираем услуги только из хостела, соответствующего комнате
        available_services = services_by_hostel.get(hostel_id, [])
        if not available_services:
            continue  # Если нет доступных услуг, пропускаем итерацию

        service_id = random.choice(available_services)
        cnt = random.randint(1, 5)
        service_accommodations.append((service_id[0], accommodation_id[0], cnt))  # Используем id услуги и id проживания

    return service_accommodations

# Функция для получения ID хостела по ID комнаты
def get_hostel_id_by_room(room_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT hostel_id FROM rooms WHERE id = ?", room_id)
    hostel_id = cursor.fetchone()
    cursor.close()
    conn.close()

    return hostel_id[0] if hostel_id else None

# Функция для вставки данных в базу данных
def insert_data_to_db(data, table):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    if table == 'employee':
        cursor.executemany(
            "INSERT INTO employee (id, full_name, date_of_birth) VALUES (?, ?, ?)",
            data
        )
    elif table == 'client_registration_card':
        cursor.executemany(
            "INSERT INTO client_registration_card (id, full_name, phone, gender, employee_id) VALUES (?, ?, ?, ?, ?)",
            data
        )
    elif table == 'hostel':
        cursor.executemany(
            "INSERT INTO hostel (id, city, name) VALUES (?, ?, ?)",
            data
        )
    elif table == 'rooms':
        cursor.executemany(
            "INSERT INTO rooms (id, type_of_gender, number_of_seats, type_comfort, hostel_id) VALUES (?, ?, ?, ?, ?)",
            data
        )
    elif table == 'seat':
        cursor.executemany(
            "INSERT INTO seat (id, room_id) VALUES (?, ?)",
            data
        )
    elif table == 'reservation':
        cursor.executemany(
            "INSERT INTO reservation (client_id, seat_id, start_date, end_date) VALUES (?, ?, ?, ?)",
            data
        )
    elif table == 'price':
        cursor.executemany(
            "INSERT INTO price (id, price, start_date, end_date) VALUES (?, ?, ?, ?)",
            data
        )
    elif table == 'service':
        cursor.executemany(
            "INSERT INTO service (id, name, price_service, hostel_id) VALUES (?, ?, ?, ?)",
            data
        )
    elif table == 'accommodation':
        cursor.executemany(
            "INSERT INTO accommodation (accommodation_id, client_id, room_id, start_date, end_date) VALUES (?, ?, ?, ?, ?)",
            data
        )
    elif table == 'service_accommodation':
        cursor.executemany(
            "INSERT INTO service_accommodation (service_id, accommodation_id, cnt) VALUES (?, ?, ?)",
            data
        )


    conn.commit()
    cursor.close()
    conn.close()

# Основная функция генерации данных
def generate_data(num_employees = 0,num_clients = 0, num_hostels = 0, num_rooms = 0, num_reservations = 0, num_prices = 0, num_services = 0, num_accommodations = 0, num_service_accs = 3):
    employees = generate_employees(num_employees)
    clients = generate_clients(num_clients,get_existing_employees())
    hostels = generate_hostels(num_hostels)
    rooms = generate_rooms(num_rooms,get_existing_comforts(),get_existing_hostels())
    #seats = generate_seats(get_existing_rooms())
    reservations = generate_reservations(num_reservations,get_existing_clients(), get_existing_rooms())#get_existing_seats())
    prices = generate_prices(num_prices)
    services = generate_services(num_services,get_existing_hostels(),get_existing_prices())
    accommodations = generate_accommodations(num_accommodations,get_existing_clients(), get_existing_rooms())
    service_accs = generate_service_accommodations(num_service_accs,get_existing_services(),get_existing_accommodations())


    # Вставка данных в базу данных с использованием потоков
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(insert_data_to_db, employees, 'employee')
        #print(employees)
        executor.submit(insert_data_to_db, clients, 'client_registration_card')
        #print(clients)
        executor.submit(insert_data_to_db,hostels, 'hostel')
        #print(hostels)
        executor.submit(insert_data_to_db, rooms, 'rooms')
        #for room in rooms: print(room)
        #executor.submit(insert_data_to_db, seats, 'seat')
        executor.submit(insert_data_to_db, reservations, 'reservation')
        #for reservation in reservations: print(reservation)
        executor.submit(insert_data_to_db, prices, 'price')
        #for price in prices: print(price)
        executor.submit(insert_data_to_db, services, 'service')
        #for service in services: print(service)
        executor.submit(insert_data_to_db, accommodations, 'accommodation')
        #for accommodation in accommodations: print(accommodation)
        executor.submit(insert_data_to_db, service_accs, 'service_accommodation')
        for service_accommodation in service_accs: print(service_accommodation)

if __name__ == "__main__":
    generate_data()
    #print(generate_employees(generate_employees(1)))
    #for client in get_existing_clients(): print(get_existing_clients().get(client))
def get_employee(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employee WHERE id = ?", id)

    employee = cursor.fetchall()

    cursor.close()
    conn.close()
    return employee
