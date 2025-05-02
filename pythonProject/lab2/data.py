from pythonProject.lab1.generator import *

employees_ids = get_existing_employees()
#print(employees_ids)
clients_ids = list(get_existing_clients().keys())
#print(clients_ids)
hostels_ids = get_existing_hostels()
#print(hostels_ids)
rooms_ids = list(get_existing_rooms().keys())
#print(rooms_ids)
seats_ids = get_seats()
#print(seats_ids)
prices_ids = get_existing_prices()
#print(prices_ids)
services_ids = get_services()
#print(services_ids)
services_names = ["Wi-Fi", "Breakfast", "Dinner", "Transfer service", "Room cleaning",
                             "Laundry", "Fitness room", "Spa treatments", "Bike rental",
                             "Excursions", "Bar", "Swimming pool", "Parking", "Conference room",
                             "Cable TV", "Coffee bar"]
accommodations_ids = get_accommodations()
#print(accommodations_ids)

tables_for_create = [
    #('employees', {'num_employees': random.randint(1, 5)}),
    ('clients', {'num_clients': random.randint(1, 10)}),
    # ('hostels', {'num_hostels': 1}),
    # ('rooms', {'num_rooms': 1}),
    # ('seats', {}),
    ('reservations', {'num_reservations': random.randint(1, 20)}),
    #('prices', {'num_prices': random.randint(1, 10)}),
    # ('services', {'num_services': random.randint(1, 5)}),
    ('accommodations', {'num_accommodations': random.randint(1, 20)})#,
    # ('service_accommodations', {'num_service_accommodations': random.randint(1, 5)})
]
tables_for_get = [
        # 'employees',
        # 'clients',
        # 'hostels',
        # 'rooms',
        # 'seats',
        # 'reservations',
        # 'prices',
        # 'services',
        # 'accommodations',
        # 'service_accommodations'
    'current_residence',
    'accommodation_client',
    'cnt_residence_by_day',
    'cnt_clients_of_employee',
    'reservations_last_month',
    'cnt_reservations_client',
    ]

tables_for_get_by_id = [
    ('employees', employees_ids),
    ('clients', clients_ids),
    # ('hostels', hostels_ids),
    # ('rooms', rooms_ids),
    # ('seats', seats_ids),
    # ('prices', prices_ids),
    # ('services', services_ids),
    ('accommodations', accommodations_ids),
    ('infoFromAcc', accommodations_ids),
    ('infoFromClient', clients_ids),
    ('infoFromReservation', clients_ids),
    ('accommodation_comfort', [1,2,3]),
    ('client_gender', ['Male','Female']),
]
tables_for_del_by_id = [
        # ('employees', employees_ids),
        # ('clients', clients_ids),
        # ('hostels', hostels_ids),
        # ('rooms', rooms_ids),
        # ('seats', seats_ids),
        #('prices', prices_ids),
        # ('services', services_ids),
        #('accommodations', accommodations_ids)
]