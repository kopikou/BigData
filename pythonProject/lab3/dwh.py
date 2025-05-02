import pyodbc
import pandas as pd

# Параметры подключения к исходной реляционной БД
relational_conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-I76BBVD\SQLKOPIKOU;'
    'DATABASE=Hostel;'
    'Trusted_Connection=yes;'
)

# Параметры подключения к хранилищу данных
dwh_conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-I76BBVD\SQLKOPIKOU;'
    'DATABASE=DWH;'
    'Trusted_Connection=yes;'
)

# Создание соединений
relational_conn = pyodbc.connect(relational_conn_str)
dwh_conn = pyodbc.connect(dwh_conn_str)

# ИЗМЕРЕНИЯ
def load_dim_date():
    try:
        # Определяем диапазон дат
        start_date = pd.to_datetime('1995-01-01')
        end_date = pd.to_datetime('2025-12-31')

        # Создаем DataFrame со всеми датами в диапазоне
        dates = pd.date_range(start_date, end_date, freq='D')
        df = pd.DataFrame({'full_date': dates})

        # Преобразуем дату в целочисленный ключ (формат YYYYMMDD)
        df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype('int32')

        # Извлекаем компоненты даты
        df['day_of_week'] = (df['full_date'].dt.dayofweek + 1).astype('int8')  # 1-7 instead of 0-6 (tinyint)
        df['day_name'] = df['full_date'].dt.day_name().str.slice(0, 10)  # varchar(10)
        df['day_of_month'] = df['full_date'].dt.day.astype('int8')  # tinyint
        df['day_of_year'] = df['full_date'].dt.dayofyear.astype('int16')  # smallint
        df['week_of_year'] = df['full_date'].dt.isocalendar().week.astype('int8')  # tinyint
        df['month_name'] = df['full_date'].dt.month_name().str.slice(0, 10)  # varchar(10)
        df['month_of_year'] = df['full_date'].dt.month.astype('int8')  # tinyint
        df['quarter'] = df['full_date'].dt.quarter.astype('int8')  # tinyint
        df['year'] = df['full_date'].dt.year.astype('int16')  # smallint

        # Определяем выходные (суббота и воскресенье)
        df['is_weekend'] = df['full_date'].dt.dayofweek.isin([5, 6]).astype('bool')
        # Определяем праздники
        holidays = [
            '2025-01-01', '2025-01-07', '2025-02-23', '2025-03-08',
            '2025-05-01', '2025-05-09', '2025-06-12', '2025-09-01',
            '2025-11-04', '2025-12-31',
        ]
        df['is_holiday'] = df['full_date'].astype(str).isin(holidays).astype('bool')

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'date_key', 'full_date', 'day_of_week', 'day_name', 'day_of_month',
            'day_of_year', 'week_of_year', 'month_name', 'month_of_year',
            'quarter', 'year', 'is_weekend', 'is_holiday'
        ]]

        # Создаем курсор для MS SQL Server
        cursor = dwh_conn.cursor()

        cursor.execute("SELECT TOP 1 date_key FROM dim_date")
        if cursor.fetchone():
            cursor.execute("SELECT date_key FROM dim_date")
            rows = cursor.fetchall()
            df['date_key'] = [row[0] for row in rows]
            print("dim_date already contains data, skipping load")
            return df

        # Загружаем данные в хранилище с использованием cursor.executemany
        insert_sql = """
           INSERT INTO dim_date (
               date_key, full_date, day_of_week, day_name, day_of_month,
               day_of_year, week_of_year, month_name, month_of_year,
               quarter, year, is_weekend, is_holiday
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           """

        # Преобразуем DataFrame в список кортежей с нативными Python типами
        data = [(
            int(row.date_key),  # int
            row.full_date.date(),  # date
            int(row.day_of_week),  # tinyint
            str(row.day_name),  # varchar(10)
            int(row.day_of_month),  # tinyint
            int(row.day_of_year),  # smallint
            int(row.week_of_year),  # tinyint
            str(row.month_name),  # varchar(10)
            int(row.month_of_year),  # tinyint
            int(row.quarter),  # tinyint
            int(row.year),  # smallint
            bool(row.is_weekend),  # bit
            bool(row.is_holiday)  # bit
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} dates from {start_date.date()} to {end_date.date()}")
        return df

    except Exception as e:
        print(f"Error loading dim_date: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()


def load_dim_client():
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о клиентах и их первой дате проживания
        query = """
        WITH FirstAccommodation AS (
            SELECT 
                client_id,
                MIN(start_date) AS first_accommodation_date
            FROM accommodation
            GROUP BY client_id
        )
        SELECT 
            crc.id as client_id,
            crc.full_name,
            crc.phone,
            crc.gender,
            COALESCE(fa.first_accommodation_date, GETDATE()) as registration_date,
            crc.employee_id
        FROM client_registration_card crc
        LEFT JOIN FirstAccommodation fa ON crc.id = fa.client_id
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов с указанием типов данных
        df = pd.DataFrame.from_records(rows, columns=[
            'client_id', 'full_name', 'phone', 'gender',
            'registration_date', 'employee_id'
        ])

        # Преобразуем типы данных в соответствии с требованиями
        df = df.astype({
            'client_id': 'int64',  # bigint
            'full_name': 'str',   # nvarchar(100)
            'phone': 'str',        # nvarchar(20)
            'gender': 'str',       # nvarchar(1)
            'registration_date': 'datetime64[ns]'
        })

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit
        df['effective_date'] = df['registration_date']
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'client_id', 'full_name', 'phone', 'gender',
            'registration_date', 'current_flag', 'effective_date',
            'expiration_date'
        ]]

        # Создаем курсор для хранилища данных
        dwh_cursor = dwh_conn.cursor()

        dwh_cursor.execute("SELECT TOP 1 client_key FROM dim_client")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT client_key FROM dim_client")
            rows = dwh_cursor.fetchall()
            df['client_key'] = [row[0] for row in rows]
            print("dim_date already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_client (
            client_id, full_name, phone, gender, registration_date,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами данных
        data = [(
            int(row.client_id),                # bigint
            str(row.full_name)[:100],          # nvarchar(100)
            str(row.phone)[:20],               # nvarchar(20)
            str(row.gender)[:1] if pd.notna(row.gender) else None,  # nvarchar(1)
            row.registration_date.date(),      # date
            bool(row.current_flag),            # bit
            row.effective_date.date(),         # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} clients")
        return df

    except Exception as e:
        print(f"Error loading dim_client: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_employee():
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о сотрудниках и их первой дате обслуживания клиента
        query = """
        WITH ClientFirstAccommodation AS (
            SELECT 
                a.client_id,
                MIN(a.start_date) AS first_accommodation_date
            FROM accommodation a
            GROUP BY a.client_id
        ),
        EmployeeFirstService AS (
            SELECT 
                crc.employee_id,
                MIN(COALESCE(cfa.first_accommodation_date, GETDATE())) AS first_service_date
            FROM client_registration_card crc
            LEFT JOIN ClientFirstAccommodation cfa ON crc.id = cfa.client_id
            GROUP BY crc.employee_id
        )
        SELECT 
            e.id as employee_id,
            e.full_name,
            e.date_of_birth,
            DATEDIFF(YEAR, e.date_of_birth, GETDATE()) as age,
            efs.first_service_date as effective_date
        FROM employee e
        LEFT JOIN EmployeeFirstService efs ON e.id = efs.employee_id
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов с явным указанием типов
        df = pd.DataFrame.from_records(rows, columns=[
            'employee_id', 'full_name', 'date_of_birth',
            'age', 'effective_date'
        ])

        # Преобразуем типы данных в соответствии с требованиями
        df = df.astype({
            'employee_id': 'int64',  # bigint
            'full_name': 'str',  # nvarchar(100)
            'date_of_birth': 'datetime64[ns]',  # будет преобразовано в date
            'age': 'int32',  # int
            'effective_date': 'datetime64[ns]'  # будет преобразовано в date
        })

        # Если у сотрудника нет клиентов, используем текущую дату
        df['effective_date'] = df['effective_date'].fillna(pd.to_datetime('today'))

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'employee_id', 'full_name', 'date_of_birth', 'age',
            'current_flag', 'effective_date', 'expiration_date'
        ]]

        # Создаем курсор для хранилища данных
        dwh_cursor = dwh_conn.cursor()

        # Проверяем, есть ли уже данные в таблице dim_employee
        dwh_cursor.execute("SELECT TOP 1 employee_key FROM dim_employee")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT employee_key FROM dim_employee")
            rows = dwh_cursor.fetchall()
            df['employee_key'] = [row[0] for row in rows]
            print("dim_employee already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_employee (
            employee_id, full_name, date_of_birth, age,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами данных
        data = [(
            int(row.employee_id),  # bigint
            str(row.full_name)[:100],  # nvarchar(100)
            row.date_of_birth.date(),  # date
            int(row.age),  # int
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} employees")
        return df

    except Exception as e:
        print(f"Error loading dim_employee: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_hostel():
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о хостелах и их первой дате проживания
        query = """
        WITH HostelFirstAccommodation AS (
            SELECT 
                r.hostel_id,
                MIN(a.start_date) AS first_accommodation_date
            FROM accommodation a
            JOIN rooms r ON a.room_id = r.id
            GROUP BY r.hostel_id
        )
        SELECT 
            h.id as hostel_id,
            h.name,
            h.city,
            COALESCE(hfa.first_accommodation_date, GETDATE()) as effective_date
        FROM hostel h
        LEFT JOIN HostelFirstAccommodation hfa ON h.id = hfa.hostel_id
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов с явным указанием типов
        df = pd.DataFrame.from_records(rows, columns=[
            'hostel_id', 'name', 'city', 'effective_date'
        ])

        # Преобразуем типы данных в соответствии с требованиями
        df = df.astype({
            'hostel_id': 'int64',  # bigint
            'name': 'str',  # nvarchar(100)
            'city': 'str',  # nvarchar(50)
            'effective_date': 'datetime64[ns]'  # будет преобразовано в date
        })

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'hostel_id', 'name', 'city',
            'current_flag', 'effective_date', 'expiration_date'
        ]]

        # Создаем курсор для хранилища данных
        dwh_cursor = dwh_conn.cursor()

        # Проверяем, есть ли уже данные в таблице dim_hostel
        dwh_cursor.execute("SELECT TOP 1 hostel_key FROM dim_hostel")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT hostel_key FROM dim_hostel")
            rows = dwh_cursor.fetchall()
            df['hostel_key'] = [row[0] for row in rows]
            print("dim_hostel already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_hostel (
            hostel_id, name, city,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами данных
        data = [(
            int(row.hostel_id),  # bigint
            str(row.name)[:100],  # nvarchar(100)
            str(row.city)[:50],  # nvarchar(50)
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} hostels")
        return df

    except Exception as e:
        print(f"Error loading dim_hostel: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_comfort():
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о типах комфорта и их первой дате использования
        query = """
        WITH ComfortFirstUsage AS (
            SELECT 
                r.type_comfort as comfort_id,
                MIN(a.start_date) AS first_usage_date
            FROM accommodation a
            JOIN rooms r ON a.room_id = r.id
            GROUP BY r.type_comfort
        )
        SELECT 
            c.id as comfort_id,
            c.type,
            c.price,
            COALESCE(cfu.first_usage_date, GETDATE()) as effective_date
        FROM comfortt c
        LEFT JOIN ComfortFirstUsage cfu ON c.id = cfu.comfort_id
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов
        df = pd.DataFrame.from_records(rows, columns=[
            'comfort_id', 'type', 'price', 'effective_date'
        ])

        # Преобразуем типы данных в соответствии с требованиями
        df = df.astype({
            'comfort_id': 'int64',  # bigint
            'type': 'str',  # nvarchar(50)
            'price': 'int32',  # int
            'effective_date': 'datetime64[ns]'  # будет преобразовано в дату
        })

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'comfort_id', 'type', 'price',
            'current_flag', 'effective_date', 'expiration_date'
        ]]

        # Создаем курсор
        dwh_cursor = dwh_conn.cursor()

        # Проверяем, есть ли уже данные в таблице dim_comfort
        dwh_cursor.execute("SELECT TOP 1 comfort_key FROM dim_comfort")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT comfort_key FROM dim_comfort")
            rows = dwh_cursor.fetchall()
            df['comfort_key'] = [row[0] for row in rows]
            print("dim_comfort already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_comfort (
            comfort_id, type, price,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей
        data = [(
            int(row.comfort_id),  # bigint
            str(row.type)[:50],  # nvarchar(50)
            int(row.price),  # int
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} comfort types")
        return df

    except Exception as e:
        print(f"Error loading dim_comfort: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_room(dim_hostel, dim_comfort):
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о комнатах из исходной БД
        query = """
        SELECT 
            r.id as room_id,
            r.type_of_gender,
            r.number_of_seats,
            r.type_comfort as comfort_id,
            r.hostel_id
        FROM rooms r
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов
        df = pd.DataFrame.from_records(rows, columns=[
            'room_id', 'type_of_gender', 'number_of_seats',
            'comfort_id', 'hostel_id'
        ]).astype({
            'room_id': 'int64',
            'type_of_gender': 'str',
            'number_of_seats': 'int32',
            'comfort_id': 'int64',
            'hostel_id': 'int64'
        })

        # Сопоставляем с ключами из других измерений
        df = df.merge(
            dim_hostel[['hostel_id', 'hostel_key', 'effective_date']],
            on='hostel_id'
        )
        df = df.merge(
            dim_comfort[['comfort_id', 'comfort_key']],
            on='comfort_id'
        )

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        # Берем effective_date из соответствующего хостела
        #df['effective_date'] = df['effective_date']
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'room_id', 'hostel_key', 'type_of_gender', 'number_of_seats',
            'comfort_key', 'current_flag', 'effective_date', 'expiration_date'
        ]]

        dwh_cursor = dwh_conn.cursor()

        # Проверяем, есть ли уже данные в таблице dim_room
        dwh_cursor.execute("SELECT TOP 1 room_key FROM dim_room")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT room_key FROM dim_room")
            rows = dwh_cursor.fetchall()
            df['room_key'] = [row[0] for row in rows]
            print("dim_room already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_room (
            room_id, hostel_key, type_of_gender, number_of_seats,
            comfort_key, current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей
        data = [(
            int(row.room_id),  # bigint
            int(row.hostel_key),  # bigint
            str(row.type_of_gender)[:1],  # nvarchar(1)
            int(row.number_of_seats),  # int
            int(row.comfort_key),  # int
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Loaded {len(df)} rooms")
        return df

    except Exception as e:
        print(f"Error loading dim_room: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_seat(dim_room):
    try:
        # Создаем курсор для работы с базой данных
        cursor = relational_conn.cursor()

        # Получаем данные о местах и их статусе бронирования
        query = """
        WITH SeatStatus AS (
            SELECT 
                s.id as seat_id,
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM reservation r 
                        WHERE r.seat_id = s.id 
                        AND GETDATE() BETWEEN r.start_date AND r.end_date
                    ) THEN 'busy'
                    ELSE 'free'
                END as status
            FROM seat s
        )
        SELECT 
            s.id as seat_id,
            s.room_id,
            ss.status
        FROM seat s
        JOIN SeatStatus ss ON s.id = ss.seat_id
        """

        # Выполняем запрос и получаем данные
        cursor.execute(query)
        rows = cursor.fetchall()

        # Создаем DataFrame из результатов
        df = pd.DataFrame.from_records(rows, columns=[
            'seat_id', 'room_id', 'status'
        ]).astype({
            'seat_id': 'int64',  # bigint
            'room_id': 'int64',  # bigint
            'status': 'str'  # nvarchar(20)
        })

        # Сопоставляем с ключами из измерения комнат
        df = df.merge(
            dim_room[['room_id', 'room_key', 'effective_date']],
            on='room_id'
        )

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'seat_id', 'room_key', 'status',
            'current_flag', 'effective_date', 'expiration_date'
        ]]

        dwh_cursor = dwh_conn.cursor()

        # Проверяем, есть ли уже данные в таблице dim_seat
        dwh_cursor.execute("SELECT TOP 1 seat_key FROM dim_seat")
        if dwh_cursor.fetchone():
            dwh_cursor.execute("SELECT seat_key FROM dim_seat")
            rows = dwh_cursor.fetchall()
            df['seat_key'] = [row[0] for row in rows]
            print("dim_seat already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_seat (
            seat_id, room_key, status,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей
        data = [(
            int(row.seat_id),  # bigint
            int(row.room_key),  # bigint
            str(row.status)[:20],  # nvarchar(20)
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Successfully loaded {len(df)} seats")
        return df

    except Exception as e:
        print(f"Error loading dim_seat: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_dim_service(dim_hostel):
    try:
        # Создаем курсор для работы с базой данных
        src_cursor = relational_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        # Получаем данные об услугах и их первом использовании
        query = """
        WITH FirstServiceUsage AS (
            SELECT 
                sa.service_id,
                MIN(a.start_date) AS first_usage_date
            FROM service_accommodation sa
            JOIN accommodation a ON sa.accommodation_id = a.accommodation_id
            GROUP BY sa.service_id
        )
        SELECT 
            s.id as service_id,
            s.name,
            s.price_service as price,
            s.hostel_id,
            COALESCE(fsu.first_usage_date, GETDATE()) as effective_date
        FROM service s
        LEFT JOIN FirstServiceUsage fsu ON s.id = fsu.service_id
        """

        # Выполняем запрос и получаем данные
        src_cursor.execute(query)
        rows = src_cursor.fetchall()

        # Создаем DataFrame из результатов
        df = pd.DataFrame.from_records(rows, columns=[
            'service_id', 'name', 'price', 'hostel_id', 'effective_date'
        ]).astype({
            'service_id': 'int64',  # bigint
            'name': 'str',  # nvarchar(100)
            'price': 'int32',  # int
            'hostel_id': 'int64',  # bigint
            'effective_date': 'datetime64[ns]'  # will be converted to date
        })

        df = df.merge(
            dim_hostel[['hostel_id', 'hostel_key']],
            on='hostel_id',
        )

        # Добавляем необходимые поля для хранилища данных
        df['current_flag'] = True  # bit (boolean)
        df['expiration_date'] = pd.NaT  # NULL date

        # Упорядочиваем колонки согласно структуре таблицы
        df = df[[
            'service_id', 'name', 'price', 'hostel_key',
            'current_flag', 'effective_date', 'expiration_date'
        ]]

        # Проверяем, есть ли уже данные в таблице dim_service
        cursor = dwh_conn.cursor()
        cursor.execute("SELECT TOP 1 service_key FROM dim_service")
        if cursor.fetchone():
            cursor.execute("SELECT service_key FROM dim_service")
            rows = cursor.fetchall()
            df['service_key'] = [row[0] for row in rows]
            print("dim_service already contains data, skipping load")
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO dim_service (
            service_id, name, price, hostel_key,
            current_flag, effective_date, expiration_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей
        data = [(
            int(row.service_id),  # bigint
            str(row.name)[:100],  # nvarchar(100)
            int(row.price),  # int
            int(row.hostel_key),  # bigint
            bool(row.current_flag),  # bit
            row.effective_date.date(),  # date
            row.expiration_date.date() if pd.notna(row.expiration_date) else None  # date (NULL)
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Successfully loaded {len(df)} services")
        return df

    except Exception as e:
        print(f"Error loading dim_service: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        src_cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()

# ФАКТОВЫЕ ТАБЛИЦЫ
def load_fact_accommodation(dim_client, dim_room, dim_seat, dim_hostel, dim_employee, dim_comfort):
    try:
        # Создаем курсоры для исходной БД и хранилища данных
        src_cursor = relational_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        # Получаем данные о проживаниях и связанных бронированиях
        query = """
        WITH AccommodationWithSeat AS (
            SELECT 
                a.accommodation_id,
                a.client_id,
                a.room_id,
                COALESCE(
                    -- Если есть соответствующая бронь, берем место из брони
                    (SELECT TOP 1 r.seat_id 
                     FROM reservation r 
                     WHERE r.client_id = a.client_id
                     AND r.start_date = a.start_date),
                    -- Иначе берем первое доступное место в комнате
                    (SELECT TOP 1 s.id 
                     FROM seat s 
                     WHERE s.room_id = a.room_id
                     ORDER BY s.id)
                ) AS seat_id,
                a.start_date,
                a.end_date,
                r.type_comfort as comfort_id,
                r.hostel_id,
                crc.employee_id,
                c.price as base_price
            FROM accommodation a
            JOIN rooms r ON a.room_id = r.id
            JOIN comfortt c ON r.type_comfort = c.id
            JOIN client_registration_card crc ON a.client_id = crc.id
        ),
        ServiceCosts AS (
            -- Вычисляем стоимость услуг для каждого проживания
            SELECT 
                sa.accommodation_id,
                SUM(sa.cnt * s.price_service) as total_service_cost
            FROM service_accommodation sa
            JOIN service s ON sa.service_id = s.id
            GROUP BY sa.accommodation_id
        )
        SELECT 
            aws.accommodation_id,
            aws.client_id,
            aws.room_id,
            aws.seat_id,
            aws.hostel_id,
            aws.employee_id,
            aws.comfort_id,
            aws.start_date,
            aws.end_date,
            aws.base_price,
            COALESCE(sc.total_service_cost, 0) as total_service_cost,
            aws.base_price + COALESCE(sc.total_service_cost, 0) as total_cost
        FROM AccommodationWithSeat aws
        LEFT JOIN ServiceCosts sc ON aws.accommodation_id = sc.accommodation_id
        """

        # Выполняем запрос и получаем данные
        src_cursor.execute(query)
        rows = src_cursor.fetchall()

        # Создаем DataFrame с явным указанием типов данных
        df = pd.DataFrame.from_records(rows, columns=[
            'accommodation_id', 'client_id', 'room_id', 'seat_id',
            'hostel_id', 'employee_id', 'comfort_id', 'start_date',
            'end_date', 'base_price', 'total_service_cost', 'total_cost'
        ])

        # Обрабатываем возможные NULL в seat_id
        if df['seat_id'].isnull().any():
            # Заполняем недостающие seat_id первым доступным местом в комнате
            for idx, row in df[df['seat_id'].isnull()].iterrows():
                room_id = row['room_id']
                if pd.notna(room_id):
                    src_cursor.execute(
                        "SELECT TOP 1 id FROM seat WHERE room_id = ? ORDER BY id",
                        room_id
                    )
                    result = src_cursor.fetchone()
                    if result:
                        df.at[idx, 'seat_id'] = result[0]
        # Удаляем записи, где все равно нет seat_id (если такие остались)
        df = df.dropna(subset=['seat_id'])

        # Преобразуем типы данных
        df = df.astype({
            'accommodation_id': 'int64',
            'client_id': 'int64',
            'room_id': 'int64',
            'seat_id': 'int64',
            'hostel_id': 'int64',
            'employee_id': 'int64',
            'comfort_id': 'int64',
            'base_price': 'int32',
            'total_service_cost': 'int32',
            'total_cost': 'int32'
        })

        # Преобразуем даты в ключи (формат YYYYMMDD) - тип int
        df['date_key'] = pd.to_datetime(df['start_date']).dt.strftime('%Y%m%d').astype('int32')
        df['start_date_key'] = df['date_key']
        df['end_date_key'] = pd.to_datetime(df['end_date']).dt.strftime('%Y%m%d').astype('int32')

        # Вычисляем продолжительность проживания в днях (тип int)
        df['duration_days'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.days.astype(
            'int32')

        # Сопоставляем с ключами измерений (тип bigint)
        df = df.merge(dim_client[['client_id', 'client_key']], on='client_id')
        df = df.merge(dim_room[['room_id', 'room_key']], on='room_id')
        df = df.merge(dim_seat[['seat_id', 'seat_key']], on='seat_id')
        df = df.merge(dim_hostel[['hostel_id', 'hostel_key']], on='hostel_id')
        df = df.merge(dim_employee[['employee_id', 'employee_key']], on='employee_id')
        df = df.merge(dim_comfort[['comfort_id', 'comfort_key']], on='comfort_id')

        df1 = df['accommodation_id']
        # Выбираем только нужные колонки для фактовой таблицы
        df = df[[
            'client_key', 'room_key', 'seat_key', 'hostel_key', 'employee_key', 'comfort_key',
            'date_key', 'start_date_key', 'end_date_key', 'duration_days',
            'base_price', 'total_service_cost', 'total_cost'
        ]]
        # Проверяем, есть ли уже данные в таблице fact_accommodation
        cursor = dwh_conn.cursor()
        cursor.execute("SELECT TOP 1 accommodation_key FROM fact_accommodation")
        if cursor.fetchone():
            cursor.execute("SELECT accommodation_key FROM fact_accommodation")
            rows = cursor.fetchall()
            df['accommodation_key'] = [row[0] for row in rows]
            print("fact_accommodation already contains data, skipping load")
            df['accommodation_id'] = df1
            return df

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO fact_accommodation (
            client_key, room_key, seat_key, hostel_key, employee_key, comfort_key,
            date_key, start_date_key, end_date_key, duration_days,
            base_price, total_service_cost, total_cost
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами
        data = [(
            int(row.client_key),  # bigint
            int(row.room_key),  # bigint
            int(row.seat_key),  # bigint
            int(row.hostel_key),  # bigint
            int(row.employee_key),  # bigint
            int(row.comfort_key),  # bigint
            int(row.date_key),  # int
            int(row.start_date_key),  # int
            int(row.end_date_key),  # int
            int(row.duration_days),  # int
            int(row.base_price),  # int
            int(row.total_service_cost),  # int
            int(row.total_cost)  # int
        ) for row in df.itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Успешно загружено {len(df)} записей о проживаниях")
        return df

    except Exception as e:
        print(f"Ошибка при загрузке fact_accommodation: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        src_cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_fact_reservation(dim_client, dim_room, dim_seat, dim_hostel):
    try:
        # Создаем курсоры для исходной БД и хранилища данных
        src_cursor = relational_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        # Получаем данные о бронированиях из операционной БД
        query = """
        SELECT 
            r.client_id,
            r.seat_id,
            s.room_id,
            rm.hostel_id,
            r.start_date,
            r.end_date,
            DATEDIFF(DAY, r.start_date, r.end_date) as duration_days,
            0 as is_cancelled,
            CASE WHEN EXISTS (
                SELECT 1 FROM accommodation a 
                WHERE a.client_id = r.client_id 
                AND a.start_date BETWEEN r.start_date AND r.end_date
            ) THEN 1 ELSE 0 END as is_converted_to_accommodation
        FROM reservation r
        JOIN seat s ON r.seat_id = s.id
        JOIN rooms rm ON s.room_id = rm.id
        """

        # Выполняем запрос и получаем данные
        src_cursor.execute(query)
        rows = src_cursor.fetchall()

        # Создаем DataFrame с явным указанием типов данных
        df = pd.DataFrame.from_records(rows, columns=[
            'client_id', 'seat_id', 'room_id', 'hostel_id',
            'start_date', 'end_date', 'duration_days',
            'is_cancelled', 'is_converted_to_accommodation'
        ]).astype({
            'client_id': 'int64',  # bigint
            'seat_id': 'int64',  # bigint
            'room_id': 'int64',  # bigint
            'hostel_id': 'int64',  # bigint
            'duration_days': 'int32',  # int
            'is_cancelled': 'bool',  # bit
            'is_converted_to_accommodation': 'bool'  # bit
        })

        # Преобразуем даты в ключи (формат YYYYMMDD) - тип int
        df['date_key'] = pd.to_datetime(df['start_date']).dt.strftime('%Y%m%d').astype('int32')
        df['start_date_key'] = df['date_key']
        df['end_date_key'] = pd.to_datetime(df['end_date']).dt.strftime('%Y%m%d').astype('int32')

        # Сопоставляем с ключами измерений (тип bigint)
        df = df.merge(dim_client[['client_id', 'client_key']], on='client_id')
        df = df.merge(dim_room[['room_id', 'room_key']], on='room_id')
        df = df.merge(dim_seat[['seat_id', 'seat_key']], on='seat_id')
        df = df.merge(dim_hostel[['hostel_id', 'hostel_key']], on='hostel_id')

        # Выбираем только нужные колонки для фактовой таблицы
        fact_cols = [
            'client_key', 'seat_key', 'room_key', 'hostel_key',
            'date_key', 'start_date_key', 'end_date_key', 'duration_days',
            'is_cancelled', 'is_converted_to_accommodation'
        ]

        # Проверяем, есть ли уже данные в таблице fact_reservation
        cursor = dwh_conn.cursor()
        cursor.execute("SELECT TOP 1 reservation_key FROM fact_reservation")
        if cursor.fetchone():
            cursor.execute("SELECT reservation_key FROM fact_reservation")
            rows = cursor.fetchall()
            df['reservation_key'] = [row[0] for row in rows]
            print("fact_reservation already contains data, skipping load")
            return df[fact_cols]

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO fact_reservation (
            client_key, seat_key, room_key, hostel_key,
            date_key, start_date_key, end_date_key, duration_days,
            is_cancelled, is_converted_to_accommodation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами
        data = [(
            int(row.client_key),  # bigint
            int(row.seat_key),  # bigint
            int(row.room_key),  # bigint
            int(row.hostel_key),  # bigint
            int(row.date_key),  # int
            int(row.start_date_key),  # int
            int(row.end_date_key),  # int
            int(row.duration_days),  # int
            bool(row.is_cancelled),  # bit
            bool(row.is_converted_to_accommodation)  # bit
        ) for row in df[fact_cols].itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Успешно загружено {len(df)} записей о бронированиях")
        return df[fact_cols]

    except Exception as e:
        print(f"Ошибка при загрузке fact_reservation: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        src_cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()


def load_fact_service_usage(dim_service, fact_accommodation, dim_client):
    try:
        # Создаем курсоры для исходной БД и хранилища данных
        src_cursor = relational_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        # Получаем данные об использовании услуг с актуальными ценами
        query = """
        WITH CurrentServicePrices AS (
            -- Находим актуальные цены на услуги
            SELECT 
                p.id as price_id,
                p.price as current_price,
                s.id as service_id
            FROM price p
            JOIN service s ON p.id = s.price_service
        )
        SELECT 
            sa.service_id,
            sa.accommodation_id,
            a.client_id,
            a.start_date as usage_date,
            sa.cnt as quantity,
            csp.current_price as unit_price,
            sa.cnt * csp.current_price as total_price
        FROM service_accommodation sa
        JOIN accommodation a ON sa.accommodation_id = a.accommodation_id
        JOIN CurrentServicePrices csp ON sa.service_id = csp.service_id
        """

        # Выполняем запрос и получаем данные
        src_cursor.execute(query)
        rows = src_cursor.fetchall()

        # Создаем DataFrame с явным указанием типов данных
        df = pd.DataFrame.from_records(rows, columns=[
            'service_id', 'accommodation_id', 'client_id', 'usage_date',
            'quantity', 'unit_price', 'total_price'
        ]).astype({
            'service_id': 'int64',  # bigint
            'accommodation_id': 'int64',  # bigint
            'client_id': 'int64',  # bigint
            'quantity': 'int32',  # int
            'unit_price': 'int32',  # int
            'total_price': 'int32'  # int
        })

        # Преобразуем дату в ключ (формат YYYYMMDD) - тип int
        df['date_key'] = pd.to_datetime(df['usage_date']).dt.strftime('%Y%m%d').astype('int32')

        # Сопоставляем с ключами измерений (тип bigint)
        df = df.merge(dim_service[['service_id', 'service_key']], on='service_id')
        df = df.merge(fact_accommodation[['accommodation_id', 'accommodation_key']], on='accommodation_id')
        df = df.merge(dim_client[['client_id', 'client_key']], on='client_id')

        # Выбираем только нужные колонки для фактовой таблицы
        fact_cols = [
            'service_key', 'accommodation_key', 'client_key', 'date_key',
            'quantity', 'unit_price', 'total_price'
        ]

        # Проверяем, есть ли уже данные в таблице fact_service_usage
        cursor = dwh_conn.cursor()
        cursor.execute("SELECT TOP 1 service_usage_key FROM fact_service_usage")
        if cursor.fetchone():
            print("fact_service_usage already contains data, skipping load")
            return df[fact_cols]

        # Подготавливаем SQL для вставки
        insert_sql = """
        INSERT INTO fact_service_usage (
            service_key, accommodation_key, client_key, date_key,
            quantity, unit_price, total_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Преобразуем DataFrame в список кортежей с правильными типами
        data = [(
            int(row.service_key),  # bigint
            int(row.accommodation_key),  # bigint
            int(row.client_key),  # bigint
            int(row.date_key),  # int
            int(row.quantity),  # int
            int(row.unit_price),  # int
            int(row.total_price)  # int
        ) for row in df[fact_cols].itertuples()]

        # Выполняем пакетную вставку
        dwh_cursor.executemany(insert_sql, data)
        dwh_conn.commit()

        print(f"Успешно загружено {len(df)} записей об использовании услуг")
        return df[fact_cols]

    except Exception as e:
        print(f"Ошибка при загрузке fact_service_usage: {str(e)}")
        dwh_conn.rollback()
        raise
    finally:
        src_cursor.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()

def main():
    try:
        # Загрузка измерений

        # print("Loading dim_date...")
        load_dim_date()

        # print("Loading dim_client...")
        dim_client = load_dim_client()

        # print("Loading dim_employee...")
        dim_employee = load_dim_employee()

        # print("Loading dim_hostel...")
        dim_hostel = load_dim_hostel()
        #print(dim_hostel)

        # print("Loading dim_comfort...")
        dim_comfort = load_dim_comfort()
        #print(dim_comfort)

        #print("Loading dim_room...")
        dim_room = load_dim_room(dim_hostel, dim_comfort)

        #print("Loading dim_seat...")
        dim_seat = load_dim_seat(dim_room)

        #print("Loading dim_service...")
        dim_service = load_dim_service(dim_hostel)

        #print("Loading fact_accommodation...")
        fact_accommodation = load_fact_accommodation(dim_client, dim_room, dim_seat, dim_hostel, dim_employee, dim_comfort)

        #print("Loading fact_reservation...")
        load_fact_reservation(dim_client, dim_room, dim_seat, dim_hostel)

        print("Loading fact_service_usage...")
        load_fact_service_usage(dim_service, fact_accommodation, dim_client)

        print("Data loading completed successfully!")

    except Exception as e:
        print(f"Error during data loading: {str(e)}")
    finally:
        relational_conn.close()
        dwh_conn.close()

if __name__ == "__main__":
    main()