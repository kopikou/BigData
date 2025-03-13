from datetime import timedelta

from faker import Faker
from flask import Flask, request, jsonify
import pyodbc
from pythonProject.generator import *

app = Flask(__name__)
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-I76BBVD\SQLKOPIKOU;'
    'DATABASE=Hostel;'
    'Trusted_Connection=yes;'
)
fake = Faker()


@app.route('/employees', methods=['POST'])
def create_employees():
    num_employees = int(request.json['num_employees'])
    employees = generate_employees(num_employees)
    insert_data_to_db(employees, 'employee')
    return jsonify({'message': f'Successfully generated {len(employees)} employees.'})
@app.route('/employees', methods=['GET'])
def get_employees():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employee")

    employees = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{'message': f'Successfully got employees: ',"id": employee.id, "full_name": employee.full_name, "date_of_birth": employee.date_of_birth} for employee in employees])
@app.route('/employees/<int:id>', methods=['GET'])
def get_employee(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employee WHERE id = ?", id)

    employee = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully got employee by id',"id": employee[0][0], "full_name": employee[0][1], "date_of_birth": employee[0][2]})
@app.route('/employees/<int:id>', methods=['PUT'])
def update_employee(id):
    full_name = request.json['full_name']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE employee SET full_name = ? WHERE id = ?", (full_name, id))

    # employee = cursor.fetchall()
    # employee[0][1] = full_name
    conn.commit()
    cursor.execute("SELECT * FROM employee WHERE id = ?", id)
    employee = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully updated employee',"id": employee[0][0], "full_name": employee[0][1], "date_of_birth": employee[0][2]})
@app.route('/employees/<int:id>', methods=['DELETE'])
def delete_employee(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM employee WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted employee with id = {id}.'})

@app.route('/clients', methods=['POST'])
def create_clients():
    num_clients = int(request.json['num_clients'])
    existing_employees = get_existing_employees()
    clients = generate_clients(num_clients, existing_employees)
    insert_data_to_db(clients, 'client_registration_card')
    return jsonify({'message': f'Successfully generated {len(clients)} clients.'})
@app.route('/clients', methods=['GET'])
def get_clients():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM client_registration_card")

    clients = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{'message': f'Successfully got clients',"id": client.id, "full_name": client.full_name, "phone": client.phone, "gender": client.gender,
                     "employee_id": client.employee_id} for client in clients])
@app.route('/clients/<int:id>', methods=['GET'])
def get_client(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM client_registration_card WHERE id = ?", id)

    client = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully got client by id',"id": client[0][0], "full_name": client[0][1], "phone": client[0][2], "gender": client[0][3],
                     "employee_id": client[0][4]})
@app.route('/clients/<int:id>', methods=['PUT'])
def update_client(id):
    phone = request.json['phone']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE client_registration_card SET phone = ? WHERE id = ?", (phone, id))

    conn.commit()
    cursor.execute("SELECT * FROM client_registration_card WHERE id = ?", id)
    client = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully updated client',"id": client[0][0], "full_name": client[0][1], "phone": client[0][2], "gender": client[0][3],
                     "employee_id": client[0][4]})
@app.route('/clients/<int:id>', methods=['DELETE'])
def delete_client(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM client_registration_card WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted client with id = {id}.'})

@app.route('/hostels', methods=['POST'])
def create_hostels():
    num_hostels = int(request.json['num_hostels'])
    hostels = generate_hostels(num_hostels)
    insert_data_to_db(hostels,'hostel')
    return jsonify({'message': f'Successfully generated {len(hostels)} hostels.'})
@app.route('/hostels', methods=['GET'])
def get_hostels():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM hostel")

    hostels = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"id": hostel.id, "city": hostel.city, "name": hostel.name} for hostel in hostels])
@app.route('/hostels/<int:id>', methods=['GET'])
def get_hostel(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM hostel WHERE id = ?", id)

    hostel = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"id": hostel[0][0], "city": hostel[0][1], "name": hostel[0][1]})
@app.route('/hostels/<int:id>', methods=['PUT'])
def update_hostel(id):
    name = request.json['name']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE hostel SET name = ? WHERE id = ?", (name, id))

    conn.commit()
    cursor.execute("SELECT * FROM hostel WHERE id = ?", id)
    hostel = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"id": hostel[0][0], "city": hostel[0][1], "name": hostel[0][1]})
@app.route('/hostels/<int:id>', methods=['DELETE'])
def delete_hostel(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM hostel WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted hostel with id = {id}.'})

@app.route('/rooms', methods=['POST'])
def create_rooms():
    num_rooms = int(request.json['num_rooms'])
    exiting_comforts = get_existing_comforts()
    exiting_hostels = get_existing_hostels()
    rooms = generate_rooms(num_rooms, exiting_comforts, exiting_hostels)
    insert_data_to_db(rooms, 'rooms')
    return jsonify({'message': f'Successfully generated {len(rooms)} rooms.'})
@app.route('/rooms', methods=['GET'])
def get_rooms():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM rooms")

    rooms = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"id": room.id, "type_of_gender": room.type_of_gender, "number_of_seats": room.number_of_seats,
                     "type_comfort": room.type_comfort, "hostel_id": room.hostel_id} for room in rooms])
@app.route('/rooms/<int:id>', methods=['GET'])
def get_room(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM rooms WHERE id = ?", id)

    room = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"id": room[0][0], "type_of_gender": room[0][1], "number_of_seats": room[0][2],
                     "type_comfort": room[0][3], "hostel_id": room[0][4]})
@app.route('/rooms/<int:id>', methods=['PUT'])
def update_room(id):
    type_comfort = request.json['type_comfort']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE rooms SET type_comfort = ? WHERE id = ?", (type_comfort, id))

    conn.commit()
    cursor.execute("SELECT * FROM rooms WHERE id = ?", id)
    room = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"id": room[0][0], "type_of_gender": room[0][1], "number_of_seats": room[0][2],
                     "type_comfort": room[0][3], "hostel_id": room[0][4]})
@app.route('/rooms/<int:id>', methods=['DELETE'])
def delete_room(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rooms WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted room with id = {id}.'})

@app.route('/seats', methods=['POST'])
def create_seats():
    exiting_rooms = get_existing_rooms()
    seats = generate_seats(exiting_rooms)
    insert_data_to_db(seats, 'seat')
    return jsonify({'message': f'Successfully generated {len(seats)} seats.'})
@app.route('/seats', methods=['GET'])
def get_seats():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM seat")

    seats = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"id": seat.id, "room_id": seat.room_id} for seat in seats])
@app.route('/seats/<int:id>', methods=['GET'])
def get_seat(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM seat WHERE id = ?", id)

    seat = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"id": seat[0][0], "room_id": seat[0][1]})
@app.route('/seats/<int:id>', methods=['DELETE'])
def delete_seat(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM seat WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted seat with id = {id}.'})

@app.route('/reservations', methods=['POST'])
def create_reservations():
    num_reservations = int(request.json['num_reservations'])
    exiting_clients = get_existing_clients()
    exiting_rooms = get_existing_rooms()
    reservations = generate_reservations(num_reservations, exiting_clients, exiting_rooms)
    insert_data_to_db(reservations, 'reservation')
    return jsonify({'message': f'Successfully generated {len(reservations)} reservations.'})
@app.route('/reservations', methods=['GET'])
def get_reservations():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reservation")

    reservations = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"client_id": reservation.client_id, "seat_id": reservation.seat_id,
                     "start_date": reservation.start_date, "end_date": reservation.end_date} for reservation in reservations])
@app.route('/reservations/<int:client_id>/<int:seat_id>', methods=['GET'])
def get_reservation(client_id, seat_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reservation WHERE client_id = ? AND seat_id = ?", (client_id, seat_id))

    reservation = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"client_id": reservation[0][0], "seat_id": reservation[0][1],
                     "start_date": reservation[0][2], "end_date": reservation[0][3]})
@app.route('/reservations/<int:client_id>/<int:seat_id>', methods=['PUT'])
def update_reservation(client_id, seat_id):
    start_date = request.json['start_date']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("SELECT start_date FROM reservation WHERE client_id = ? AND seat_id = ?", (client_id, seat_id))
    current_date = cursor.fetchall()

    cursor.execute("UPDATE reservation SET start_date = ? WHERE client_id = ? AND seat_id = ?",
                   (fake.date_time_between(start_date=current_date[0][0], end_date=current_date[0][0]) - timedelta(days=1), client_id, seat_id))

    conn.commit()
    cursor.execute("SELECT * FROM reservation WHERE client_id = ? AND seat_id = ?", (client_id, seat_id))
    reservation = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"client_id": reservation[0][0], "seat_id": reservation[0][1],
                     "start_date": reservation[0][2], "end_date": reservation[0][3]})
@app.route('/reservations/<int:client_id>/<int:seat_id>', methods=['DELETE'])
def delete_reservation(client_id, seat_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM reservation WHERE client_id = ? AND seat_id = ?", (client_id, seat_id))

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted reservation with client_id = {client_id} and seat_id = {seat_id}.'})

@app.route('/prices', methods=['POST'])
def create_prices():
    num_prices = int(request.json['num_prices'])
    prices = generate_prices(num_prices)
    insert_data_to_db(prices, 'price')
    return jsonify({'message': f'Successfully generated {len(prices)} prices.'})
@app.route('/prices', methods=['GET'])
def get_prices():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM price")

    prices = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"id": price.id, "price": price.price, "start_date": price.start_date, "end_date": price.end_date} for price in prices])
@app.route('/prices/<int:id>', methods=['GET'])
def get_price(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM price WHERE id = ?", id)

    price = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"id": price[0][0], "price": price[0][1], "start_date": price[0][2], "end_date": price[0][3]})
@app.route('/prices/<int:id>', methods=['PUT'])
def update_price(id):
    price_r = request.json['price']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE price SET price = ? WHERE id = ?", (price_r, id))

    conn.commit()
    cursor.execute("SELECT * FROM price WHERE id = ?", id)
    price = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"id": price[0][0], "price": price[0][1], "start_date": price[0][2], "end_date": price[0][3]})
@app.route('/prices/<int:id>', methods=['DELETE'])
def delete_price(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM price WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted price with id = {id}.'})

@app.route('/services', methods=['POST'])
def create_services():
    num_services = int(request.json['num_services'])
    exiting_hostels = get_existing_hostels()
    exiting_prices = get_existing_prices()
    services = generate_services(num_services, exiting_hostels, exiting_prices)
    insert_data_to_db(services, 'service')
    return jsonify({'message': f'Successfully generated {len(services)} services.'})
@app.route('/services', methods=['GET'])
def get_services():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM service")

    services = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"id": service.id, "name": service.name, "price_service": service.price_service,
                     "hostel_id": service.hostel_id} for service in services])
@app.route('/services/<int:id>', methods=['GET'])
def get_service(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM service WHERE id = ?", id)

    service = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"id": service[0][0], "name": service[0][1], "price_service": service[0][2], "hostel_id": service[0][3]})
@app.route('/services/<int:id>', methods=['PUT'])
def update_service(id):
    name = request.json['name']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("UPDATE service SET name = ? WHERE id = ?", (name, id))

    conn.commit()
    cursor.execute("SELECT * FROM service WHERE id = ?", id)
    service = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"id": service[0][0], "name": service[0][1], "price_service": service[0][2], "hostel_id": service[0][3]})
@app.route('/services/<int:id>', methods=['DELETE'])
def delete_service(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM service WHERE id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted service with id = {id}.'})

@app.route('/accommodations', methods=['POST'])
def create_accommodations():
    num_accommodations = int(request.json['num_accommodations'])
    exiting_clients = get_existing_clients()
    exiting_rooms = get_existing_rooms()
    accommodations = generate_accommodations(num_accommodations, exiting_clients, exiting_rooms)
    insert_data_to_db(accommodations, 'accommodation')
    return jsonify({'message': f'Successfully generated {len(accommodations)} accommodations.'})
@app.route('/accommodations', methods=['GET'])
def get_accommodations():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accommodation")

    accommodations = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"accommodation_id": accommodation.accommodation_id, "client_id": accommodation.client_id, "room_id": accommodation.room_id,
                     "start_date": accommodation.start_date, "end_date": accommodation.end_date} for accommodation in accommodations])
@app.route('/accommodations/<int:id>', methods=['GET'])
def get_accommodation(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accommodation WHERE accommodation_id = ?", id)

    accommodation = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"accommodation_id": accommodation[0][0], "client_id": accommodation[0][1], "room_id": accommodation[0][2],
                     "start_date": accommodation[0][3], "end_date": accommodation[0][4]})
@app.route('/accommodations/<int:id>', methods=['PUT'])
def update_accommodation(id):
    start_date = request.json['start_date']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("SELECT start_date FROM accommodation WHERE accommodation_id = ?", id)
    current_date = cursor.fetchall()

    cursor.execute("UPDATE accommodation SET start_date = ? WHERE accommodation_id = ?",
                   (fake.date_time_between(start_date=current_date[0][0], end_date=current_date[0][0]) - timedelta(days=1), id))

    conn.commit()
    cursor.execute("SELECT * FROM accommodation WHERE accommodation_id = ?", id)
    accommodation = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"accommodation_id": accommodation[0][0], "client_id": accommodation[0][1], "room_id": accommodation[0][2],
                     "start_date": accommodation[0][3], "end_date": accommodation[0][4]})
@app.route('/accommodations/<int:id>', methods=['DELETE'])
def delete_accommodation(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM accommodation WHERE accommodation_id = ?", id)

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted accommodation with accommodation_id = {id}.'})

@app.route('/service_accommodations', methods=['POST'])
def create_service_accommodations():
    num_service_accommodations = int(request.json['num_service_accommodations'])
    exiting_services = get_existing_services()
    exiting_accommodations = get_existing_accommodations()
    service_accommodations = generate_service_accommodations(num_service_accommodations, exiting_services, exiting_accommodations)
    insert_data_to_db(service_accommodations, 'service_accommodation')
    return jsonify({'message': f'Successfully generated {len(service_accommodations)} service_accommodations.'})
@app.route('/service_accommodations', methods=['GET'])
def get_service_accommodations():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM service_accommodation")

    service_accommodations = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"service_id": service_accommodation.service_id, "accommodation_id": service_accommodation.accommodation_id,
                     "cnt": service_accommodation.cnt} for service_accommodation in service_accommodations])
@app.route('/service_accommodations/<int:service_id>/<int:accommodation_id>', methods=['GET'])
def get_service_accommodation(service_id, accommodation_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM service_accommodation WHERE service_id = ? AND accommodation_id = ?", (service_id, accommodation_id))

    service_accommodation = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"service_id": service_accommodation[0][0], "accommodation_id": service_accommodation[0][1],
                     "cnt": service_accommodation[0][2]})
@app.route('/service_accommodations/<int:service_id>/<int:accommodation_id>', methods=['PUT'])
def update_service_accommodation(service_id, accommodation_id):
    cnt = request.json['cnt']
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("UPDATE service_accommodation SET cnt = ? WHERE service_id = ? AND accommodation_id = ?",(cnt, service_id, accommodation_id))

    conn.commit()
    cursor.execute("SELECT * FROM service_accommodation WHERE service_id = ? AND accommodation_id = ?", (service_id, accommodation_id))
    service_accommodation = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({"service_id": service_accommodation[0][0], "accommodation_id": service_accommodation[0][1],
                     "cnt": service_accommodation[0][2]})
@app.route('/service_accommodations/<int:service_id>/<int:accommodation_id>', methods=['DELETE'])
def delete_service_accommodation(service_id, accommodation_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM service_accommodation WHERE service_id = ? AND accommodation_id = ?", (service_id, accommodation_id))

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': f'Successfully deleted service_accommodation with service_id = {service_id} and accommodation_id = {accommodation_id}.'})

@app.route('/infoFromAcc/<int:id>', methods=['GET'])
def get_info_from_acc(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT c.full_name AS client_name, c.phone AS client_phone, e.full_name AS employee_name, r.id AS room_id, r.type_of_gender, r.number_of_seats, co.type AS comfort_type, co.price AS comfort_price, a.start_date, a.end_date FROM accommodation AS a JOIN client_registration_card AS c ON a.client_id = c.id JOIN employee AS e ON c.employee_id = e.id JOIN rooms AS r ON a.room_id = r.id JOIN comfortt AS co ON r.type_comfort = co.id WHERE a.accommodation_id = ? ORDER BY a.start_date;", id)

    info = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify({"client_name": info[0][0], "client_phone": info[0][1], "employee_name": info[0][2],
                     "room_id": info[0][3], "type_of_gender": info[0][4], "number_of_seats": info[0][5], "comfort_type": info[0][6]
                    , "comfort_price": info[0][7], "start_date": info[0][8], "end_date": info[0][9]})

@app.route('/infoFromClient/<int:id>', methods=['GET'])
def get_info_from_client(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT a.accommodation_id AS accommodation_id, a.start_date, a.end_date, r.id AS room_id, r.type_of_gender, r.number_of_seats, co.type AS comfort_type, co.price AS comfort_price FROM accommodation AS a JOIN rooms AS r ON a.room_id = r.id JOIN comfortt AS co ON r.type_comfort = co.id WHERE a.client_id = ?;", id)

    infos = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"accommodation_id": info.accommodation_id, "start_date": info.start_date, "end_date": info.end_date,
                     "room_id": info.room_id, "type_of_gender": info.type_of_gender, "number_of_seats": info.number_of_seats,
                     "comfort_type": info.comfort_type, "comfort_price": info.comfort_price} for info in infos])

@app.route('/infoFromReservation/<int:id>', methods=['GET'])
def get_info_from_reservation(id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT r.client_id, r.seat_id,r.start_date, r.end_date,s.room_id,ro.type_of_gender,ro.number_of_seats,h.id AS hostel_id,h.city FROM reservation AS r JOIN seat AS s ON r.seat_id = s.id JOIN rooms AS ro ON s.room_id = ro.id JOIN hostel AS h ON ro.hostel_id = h.id WHERE r.client_id = ?;", id)

    infos = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify([{"client_id": info.client_id, "seat_id": info.seat_id, "start_date": info.start_date,
                     "end_date": info.end_date, "room_id": info.room_id, "type_of_gender": info.type_of_gender,
                     "number_of_seats": info.number_of_seats, "hostel_id": info.hostel_id, "city": info.city} for info in infos])
if __name__ == '__main__':
    app.run(debug=True)

