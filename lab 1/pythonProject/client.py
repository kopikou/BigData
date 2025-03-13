import requests
import threading
import time
from pythonProject.data import *

fake = Faker()
base_url = 'http://localhost:5000/'

def send_post_request(endpoint, data):
    response = requests.post(f'{base_url}/{endpoint}', json=data)
    print(response.text)
def send_get_request(endpoint):
    response = requests.get(f'{base_url}/{endpoint}')
    print(response.text)
def send_get_by_id_request(endpoint, id):
    response = requests.get(f'{base_url}/{endpoint}/{id}')
    print(response.text)
def send_get_by_id_id_request(endpoint, client_id, seat_id):
    response = requests.get(f'{base_url}/{endpoint}/{client_id}/{seat_id}')
    print(response.text)
def send_put_request(endpoint, id, data):
    response = requests.put(f'{base_url}/{endpoint}/{id}', json=data)
    print(response.text)
def send_put_by_id_request(endpoint, client_id, seat_id, data):
    response = requests.put(f'{base_url}/{endpoint}/{client_id}/{seat_id}', json=data)
    print(response.text)
def send_delete_request(endpoint, id):
    response = requests.delete(f'{base_url}/{endpoint}/{id}')
    print(response.text)
def send_delete_by_id_request(endpoint, client_id, seat_id):
    response = requests.delete(f'{base_url}/{endpoint}/{client_id}/{seat_id}')
    print(response.text)


def worker():
    cnt = 0
    while cnt < 3:
        operation = random.choice(['create', 'get', 'get_by_id', 'update', 'delete'])
        if operation == 'create' and random.random() < 0.3:
            # Рандомно выбираем одну таблицу и соответствующие данные
            selected_table, data = random.choice(tables_for_create)
            print('create: ' + selected_table)
            # Отправляем POST запрос к выбранной таблице с данными
            send_post_request(selected_table, data)
        elif operation == 'get':
            # Рандомно выбираем одну таблицу
            selected_table = random.choice(tables_for_get)
            print('get: ' + selected_table)
            # Отправляем GET запрос к выбранной таблице
            send_get_request(selected_table)
        elif operation == 'get_by_id':
            # Рандомно выбираем одну таблицу и её ID
            selected_table, selected_ids = random.choice(tables_for_get_by_id)
            print('get_by_id: ' + selected_table)
            # Отправляем GET запрос к выбранной таблице
            send_get_by_id_request(selected_table, random.choice(selected_ids))
        elif operation == 'update' and random.random() < 0.3:
            tables = {
                'employees': lambda: send_put_request('employees', random.choice(employees_ids),
                                                      {'full_name': fake.name()}),
                'clients': lambda: send_put_request('clients', random.choice(clients_ids),
                                                    {'phone': fake.phone_number()}),
                'hostels': lambda: send_put_request('hostels', random.choice(hostels_ids), {'name': fake.company()}),
                # 'rooms': lambda: send_put_request('rooms', random.choice(rooms_ids),
                #                                   {'type_comfort': random.choice(["Luxury", "Standard", "Economy"])}),
                # 'prices': lambda: send_put_request('prices', random.choice(prices_ids),
                #                                    {'price': random.randint(500, 5000)}),
                # 'services': lambda: send_put_request('services', random.choice(services_ids),
                #                                      {'name': random.choice(services_names)}),
                # 'accommodations': lambda: send_put_request('accommodations', random.choice(accommodations_ids), {
                #     'start_date': fake.date_between(start_date='today', end_date='+30d')}),
                # 'service_accommodations': lambda: send_put_by_id_request('service_accommodations',
                #                                                          random.choice(services_ids),
                #                                                          random.choice(accommodations_ids),
                #                                                          {'cnt': random.randint(1, 5)})
            }
            # Рандомно выбираем одну таблицу и выполняем соответствующий запрос
            selected_table = random.choice(list(tables.keys()))
            print('update: ' + selected_table)
            tables[selected_table]()
        elif operation == 'delete'  and random.random() < 0.3:
            # Рандомно выбираем одну таблицу и её ID
            selected_table, selected_ids = random.choice(tables_for_del_by_id)
            print('delete: ' + selected_table)
            # Отправляем DEL запрос к выбранной таблице
            send_delete_request(selected_table, random.choice(selected_ids))



        time.sleep(random.uniform(0.3, 1))  # Симулирует реальное поведение пользователей
        cnt += 1

# Запускаем несколько потоков для имитации нагрузки
threads = []
for i in range(5):  # Запускаем потоки
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
