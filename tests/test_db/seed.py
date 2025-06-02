from src.utils.connection import pg8000_connect_to_local, close_connection
import json
import os
from dotenv import load_dotenv


# TO DO: add correct credentials for docker container... secrets?

# def connect_to_db(env):
#     load_dotenv(env, override=True)
#     return Connection(
#         user=os.getenv("TEST_PG_USERNAME"),
#         database=os.getenv("TEST_PG_DATABASE"),
#         password=os.getenv("TEST_PG_PASSWORD"),
#         # host=os.getenv("TEST_PG_HOST"),
#         port=int(os.getenv("TEST_PG_PORT"))
#     )

def seed_db():
    print("\U0001FAB4", "Seeding Database...")

    db = pg8000_connect_to_local()
    table_list = [
        "address",
        "counterparty",
        "currency",
        "department",
        "design",
        "payment",
        "payment_type",
        "purchase_order",
        "sales_order",
        "staff",
        "transaction",
    ]
    for table in table_list:
        db.run(f"DROP TABLE IF EXISTS {table} CASCADE")

    db.run(
        """
        CREATE TABLE address (
            address_id SERIAL PRIMARY KEY,
            address_line_1 VARCHAR NOT NULL,
            address_line_2 VARCHAR,
            district VARCHAR,
            city VARCHAR NOT NULL,
            postal_code VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            phone VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE counterparty (
            counterparty_id SERIAL PRIMARY KEY,
            counterparty_legal_name VARCHAR NOT NULL,
            legal_address_id INT NOT NULL,
            commercial_contact VARCHAR,
            delivery_contact VARCHAR,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE currency (
            currency_id SERIAL PRIMARY KEY,
            currency_code VARCHAR(3) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE department (
            department_id SERIAL PRIMARY KEY,
            department_name VARCHAR NOT NULL,
            location VARCHAR,
            manager VARCHAR,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )  
        """
    )
    db.run(
        """
        CREATE TABLE design (
            design_id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            design_name VARCHAR NOT NULL,
            file_location VARCHAR NOT NULL,
            file_name VARCHAR NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE payment_type (
            payment_type_id SERIAL PRIMARY KEY,
            payment_type_name VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )   
        """
    )
    db.run(
        """
        CREATE TABLE staff (
            staff_id SERIAL PRIMARY KEY,
            first_name varchar NOT NULL,
            last_name varchar NOT NULL,
            department_id INT,
            email_address varchar NOT NULL, 
            created_at timestamp NOT NULL, 
            last_updated timestamp NOT NULL 
        )
        """
    )
    db.run(
        """
        CREATE TABLE sales_order (
            sales_order_id SERIAL PRIMARY KEY,
            created_at timestamp NOT NULL,
            last_updated timestamp NOT NULL,
            design_id INT,
            staff_id INT,
            counterparty_id INT,
            units_sold INT NOT NULL,
            unit_price NUMERIC NOT NULL,
            currency_id INT,
            agreed_delivery_date varchar NOT NULL,
            agreed_payment_date varchar NOT NULL,
            agreed_delivery_location_id INT
        )
        """
    )
    db.run(
        """
        CREATE TABLE purchase_order (
            purchase_order_id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            staff_id INT NOT NULL,
            counterparty_id INT NOT NULL,
            item_code VARCHAR NOT NULL,
            item_quantity INT NOT NULL,
            item_unit_price NUMERIC NOT NULL,
            currency_id INT NOT NULL,
            agreed_delivery_date VARCHAR NOT NULL,
            agreed_payment_date VARCHAR NOT NULL,
            agreed_delivery_location_id INT NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE transaction (
            transaction_id SERIAL PRIMARY KEY,
            transaction_type VARCHAR NOT NULL,
            sales_order_id INT,
            purchase_order_id INT,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL
        )
        """
    )
    db.run(
        """
        CREATE TABLE payment (
            payment_id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            transaction_id INT NOT NULL,
            counterparty_id INT NOT NULL,
            payment_amount NUMERIC NOT NULL,
            currency_id INT,
            payment_type_id INT NOT NULL,
            paid boolean NOT NULL,
            payment_date VARCHAR NOT NULL,
            company_ac_number INT NOT NULL,
            counterparty_ac_number INT NOT NULL
        )
        """
    )

    with open(f'tests/test_db/data/address.json', 'r') as file:
        ADDRESS_DATA = json.load(file)
        ROWS = ADDRESS_DATA['address']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO address (
                    address_id, 
                    address_line_1, 
                    address_line_2, 
                    district, 
                    city, 
                    postal_code, 
                    country, 
                    phone, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :address_id, 
                    :address_line_1, 
                    :address_line_2, 
                    :district, 
                    :city, 
                    :postal_code, 
                    :country, 
                    :phone, 
                    :created_at, 
                    :last_updated
                )
                """,
                address_id=row['address_id'],
                address_line_1=row['address_line_1'],
                address_line_2=row['address_line_2'],
                district=row['district'],
                city=row['city'],
                postal_code=row['postal_code'],
                country=row['country'],
                phone=row['phone'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/counterparty.json', 'r') as file:
        COUNTERPARTY_DATA = json.load(file)
        ROWS = COUNTERPARTY_DATA['counterparty']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO counterparty (
                    counterparty_id, 
                    counterparty_legal_name, 
                    legal_address_id, 
                    commercial_contact, 
                    delivery_contact, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :counterparty_id, 
                    :counterparty_legal_name, 
                    :legal_address_id, 
                    :commercial_contact, 
                    :delivery_contact, 
                    :created_at, 
                    :last_updated
                )
                """,
                counterparty_id=row['counterparty_id'],
                counterparty_legal_name=row['counterparty_legal_name'],
                legal_address_id=row['legal_address_id'],
                commercial_contact=row['commercial_contact'],
                delivery_contact=row['delivery_contact'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/currency.json', 'r') as file:
        CURRENCY_DATA = json.load(file)
        ROWS = CURRENCY_DATA['currency']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO currency (
                    currency_id, 
                    currency_code, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :currency_id, 
                    :currency_code, 
                    :created_at, 
                    :last_updated
                )
                """,
                currency_id=row['currency_id'],
                currency_code=row['currency_code'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/department.json', 'r') as file:
        DEPARTMENT_DATA = json.load(file)
        ROWS = DEPARTMENT_DATA['department']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO department (
                    department_id, 
                    department_name, 
                    location, 
                    manager, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :department_id, 
                    :department_name, 
                    :location, 
                    :manager, 
                    :created_at, 
                    :last_updated
                )
                """,
                department_id=row['department_id'],
                department_name=row['department_name'],
                location=row['location'],
                manager=row['manager'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/design.json', 'r') as file:
        DESIGN_DATA = json.load(file)
        ROWS = DESIGN_DATA['design']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO design (
                    design_id, 
                    created_at, 
                    last_updated, 
                    design_name, 
                    file_location, 
                    file_name
                )
                VALUES (
                    :design_id, 
                    :created_at, 
                    :last_updated, 
                    :design_name, 
                    :file_location, 
                    :file_name
                )
                """,
                design_id=row['design_id'],
                created_at=row['created_at'],
                last_updated=row['last_updated'],
                design_name=row['design_name'],
                file_location=row['file_location'],
                file_name=row['file_name']
            )
            row_count += 1

    with open(f'tests/test_db/data/payment_type.json', 'r') as file:
        PAYMENT_TYPE_DATA = json.load(file)
        ROWS = PAYMENT_TYPE_DATA['payment_type']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO payment_type (
                    payment_type_id,  
                    payment_type_name, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :payment_type_id,  
                    :payment_type_name, 
                    :created_at, 
                    :last_updated
                )
                """,
                payment_type_id=row['payment_type_id'],
                payment_type_name=row['payment_type_name'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/staff.json', 'r') as file:
        STAFF_DATA = json.load(file)
        ROWS = STAFF_DATA['staff']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO staff (
                    staff_id, 
                    first_name, 
                    last_name, 
                    department_id, 
                    email_address, 
                    created_at, 
                    last_updated
                ) 
                VALUES (
                    :staff_id, 
                    :first_name, 
                    :last_name, 
                    :department_id, 
                    :email_address, 
                    :created_at, 
                    :last_updated
                )
                """,
                staff_id=row['staff_id'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                department_id=row['department_id'],
                email_address=row['email_address'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1 

    with open(f'tests/test_db/data/purchase_order.json', 'r') as file:
        PURCHASE_ORDER_DATA = json.load(file)
        ROWS = PURCHASE_ORDER_DATA['purchase_order']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO purchase_order (
                    purchase_order_id, 
                    created_at, 
                    last_updated, 
                    staff_id, 
                    counterparty_id, 
                    item_code, 
                    item_quantity, 
                    item_unit_price, 
                    currency_id, 
                    agreed_delivery_date, 
                    agreed_payment_date, 
                    agreed_delivery_location_id
                )
                VALUES (
                    :purchase_order_id, 
                    :created_at, 
                    :last_updated, 
                    :staff_id, 
                    :counterparty_id, 
                    :item_code, 
                    :item_quantity, 
                    :item_unit_price, 
                    :currency_id, 
                    :agreed_delivery_date, 
                    :agreed_payment_date, 
                    :agreed_delivery_location_id
                )
                """,
                purchase_order_id=row['purchase_order_id'],
                created_at=row['created_at'],
                last_updated=row['last_updated'],
                staff_id=row['staff_id'],
                counterparty_id=row['counterparty_id'],
                item_code=row['item_code'],
                item_quantity=row['item_quantity'],
                item_unit_price=row['item_unit_price'],
                currency_id=row['currency_id'],
                agreed_delivery_date=row['agreed_delivery_date'],
                agreed_payment_date=row['agreed_payment_date'],
                agreed_delivery_location_id=row['agreed_delivery_location_id']
            )
            row_count += 1

    with open(f'tests/test_db/data/transaction.json', 'r') as file:
        TRANSACTION_DATA = json.load(file)
        ROWS = TRANSACTION_DATA['transaction']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO transaction (
                    transaction_id, 
                    transaction_type, 
                    sales_order_id, 
                    purchase_order_id, 
                    created_at, 
                    last_updated
                )
                VALUES (
                    :transaction_id, 
                    :transaction_type, 
                    :sales_order_id, 
                    :purchase_order_id, 
                    :created_at, 
                    :last_updated
                )
                """,
                transaction_id=row['transaction_id'],
                transaction_type=row['transaction_type'],
                sales_order_id=row['sales_order_id'],
                purchase_order_id=row['purchase_order_id'],
                created_at=row['created_at'],
                last_updated=row['last_updated']
            )
            row_count += 1

    with open(f'tests/test_db/data/payment.json', 'r') as file:
        PAYMENT_DATA = json.load(file)
        ROWS = PAYMENT_DATA['payment']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO payment (
                    payment_id, 
                    created_at, 
                    last_updated, 
                    transaction_id, 
                    counterparty_id, 
                    payment_amount, 
                    currency_id, 
                    payment_type_id, 
                    paid, 
                    payment_date, 
                    company_ac_number, 
                    counterparty_ac_number
                )
                VALUES (
                    :payment_id, 
                    :created_at, 
                    :last_updated, 
                    :transaction_id, 
                    :counterparty_id, 
                    :payment_amount, 
                    :currency_id, 
                    :payment_type_id, 
                    :paid, 
                    :payment_date, 
                    :company_ac_number, 
                    :counterparty_ac_number
                )
                """,
                payment_id=row['payment_id'],
                created_at=row['created_at'],
                last_updated=row['last_updated'],
                transaction_id=row['transaction_id'],
                counterparty_id=row['counterparty_id'],
                payment_amount=row['payment_amount'],
                currency_id=row['currency_id'],
                payment_type_id=row['payment_type_id'],
                paid=row['paid'],
                payment_date=row['payment_date'],
                company_ac_number=row['company_ac_number'],
                counterparty_ac_number=row['counterparty_ac_number']
            )
            row_count += 1

    with open(f'tests/test_db/data/sales_order.json', 'r') as file:
        SALES_ORDER_DATA = json.load(file)
        ROWS = SALES_ORDER_DATA['sales_order']
        row_count = 0
        for row in ROWS:
            db.run(
                """
                INSERT INTO sales_order (
                    sales_order_id, 
                    created_at, 
                    last_updated, 
                    design_id, 
                    staff_id, 
                    counterparty_id, 
                    units_sold, 
                    unit_price, 
                    currency_id, 
                    agreed_delivery_date, 
                    agreed_payment_date, 
                    agreed_delivery_location_id
                )
                VALUES (
                    :sales_order_id, 
                    :created_at, 
                    :last_updated, 
                    :design_id, 
                    :staff_id, 
                    :counterparty_id, 
                    :units_sold, 
                    :unit_price, 
                    :currency_id, 
                    :agreed_delivery_date, 
                    :agreed_payment_date, 
                    :agreed_delivery_location_id
                )
                """,
                sales_order_id=row['sales_order_id'],
                created_at=row['created_at'],
                last_updated=row['last_updated'],
                design_id=row['design_id'],
                staff_id=row['staff_id'],
                counterparty_id=row['counterparty_id'],
                units_sold=row['units_sold'],
                unit_price=row['unit_price'],
                currency_id=row['currency_id'],
                agreed_delivery_date=row['agreed_delivery_date'],
                agreed_payment_date=row['agreed_payment_date'],
                agreed_delivery_location_id=row['agreed_delivery_location_id']
            )
            row_count += 1




    db.close()
