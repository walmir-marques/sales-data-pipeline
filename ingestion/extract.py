import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# Carrega as variáveis do .env
load_dotenv()

# Configuração do log — vai mostrar mensagens no terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Configuração da conexão com o Postgres
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST"),
    "port":     os.getenv("POSTGRES_PORT"),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# URL base da API
BASE_URL = "https://dummyjson.com"

def fetch_products():
    log.info("Buscando produtos na API...")
    response = requests.get(f"{BASE_URL}/products?limit=200")
    response.raise_for_status()
    data = response.json()["products"]
    log.info(f"{len(data)} produtos encontrados.")
    return data


def fetch_users():
    log.info("Buscando usuários na API...")
    response = requests.get(f"{BASE_URL}/users?limit=200")
    response.raise_for_status()
    data = response.json()["users"]
    log.info(f"{len(data)} usuários encontrados.")
    return data


def fetch_carts():
    log.info("Buscando carrinhos na API...")
    response = requests.get(f"{BASE_URL}/carts?limit=200")
    response.raise_for_status()
    data = response.json()["carts"]
    log.info(f"{len(data)} carrinhos encontrados.")
    return data


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_products(products: list):
    log.info("Carregando produtos no Bronze...")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_products (
            id                  INTEGER PRIMARY KEY,
            title               TEXT,
            description         TEXT,
            price               NUMERIC,
            discount_percentage NUMERIC,
            rating              NUMERIC,
            stock               INTEGER,
            category            TEXT,
            brand               TEXT,
            _ingested_at        TIMESTAMP DEFAULT NOW()
        )
    """)

    rows = [
        (
            p.get("id"),
            p.get("title"),
            p.get("description"),
            p.get("price"),
            p.get("discountPercentage"),
            p.get("rating"),
            p.get("stock"),
            p.get("category"),
            p.get("brand")
        )
        for p in products
    ]

    execute_values(cur, """
        INSERT INTO bronze.raw_products
            (id, title, description, price, discount_percentage,
             rating, stock, category, brand)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title               = EXCLUDED.title,
            price               = EXCLUDED.price,
            stock               = EXCLUDED.stock,
            _ingested_at        = NOW()
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"{len(rows)} produtos carregados no bronze.raw_products.")
    return len(rows)


def load_users(users: list):
    log.info("Carregando usuários no Bronze...")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_users (
            id           INTEGER PRIMARY KEY,
            first_name   TEXT,
            last_name    TEXT,
            email        TEXT,
            phone        TEXT,
            age          INTEGER,
            gender       TEXT,
            city         TEXT,
            state        TEXT,
            _ingested_at TIMESTAMP DEFAULT NOW()
        )
    """)

    rows = [
        (
            u.get("id"),
            u.get("firstName"),
            u.get("lastName"),
            u.get("email"),
            u.get("phone"),
            u.get("age"),
            u.get("gender"),
            u.get("address", {}).get("city"),
            u.get("address", {}).get("state")
        )
        for u in users
    ]

    execute_values(cur, """
        INSERT INTO bronze.raw_users
            (id, first_name, last_name, email, phone, age, gender, city, state)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            email        = EXCLUDED.email,
            phone        = EXCLUDED.phone,
            _ingested_at = NOW()
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"{len(rows)} usuários carregados no bronze.raw_users.")
    return len(rows)


def load_carts(carts: list):
    log.info("Carregando pedidos no Bronze...")
    conn = get_connection()
    cur = conn.cursor()

    # Tabela de pedidos
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_orders (
            id               INTEGER PRIMARY KEY,
            user_id          INTEGER,
            total            NUMERIC,
            discounted_total NUMERIC,
            total_products   INTEGER,
            total_quantity   INTEGER,
            _ingested_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    # Tabela de itens do pedido (array aninhado explodido)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
            order_id             INTEGER,
            product_id           INTEGER,
            title                TEXT,
            price                NUMERIC,
            quantity             INTEGER,
            total                NUMERIC,
            discount_percentage  NUMERIC,
            discounted_total     NUMERIC,
            _ingested_at         TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (order_id, product_id)
        )
    """)

    order_rows = [
        (
            c.get("id"),
            c.get("userId"),
            c.get("total"),
            c.get("discountedTotal"),
            c.get("totalProducts"),
            c.get("totalQuantity")
        )
        for c in carts
    ]

    # Explodindo os itens aninhados em linhas separadas
    item_rows = [
        (
            c.get("id"),
            item.get("id"),
            item.get("title"),
            item.get("price"),
            item.get("quantity"),
            item.get("total"),
            item.get("discountPercentage"),
            item.get("discountedTotal")
        )
        for c in carts
        for item in c.get("products", [])
    ]

    execute_values(cur, """
        INSERT INTO bronze.raw_orders
            (id, user_id, total, discounted_total, total_products, total_quantity)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            total            = EXCLUDED.total,
            _ingested_at     = NOW()
    """, order_rows)

    execute_values(cur, """
        INSERT INTO bronze.raw_order_items
            (order_id, product_id, title, price, quantity,
             total, discount_percentage, discounted_total)
        VALUES %s
        ON CONFLICT (order_id, product_id) DO UPDATE SET
            quantity         = EXCLUDED.quantity,
            _ingested_at     = NOW()
    """, item_rows)

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"{len(order_rows)} pedidos e {len(item_rows)} itens carregados.")
    return len(order_rows)

def log_run(layer: str, status: str, rows: int, message: str = ""):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bronze.pipeline_runs (layer, status, rows_loaded, message)
            VALUES (%s, %s, %s, %s)
        """, (layer, status, rows, message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.warning(f"Erro ao salvar log: {e}")


def run():
    log.info("=== Iniciando ingestão Bronze ===")
    start = datetime.now()

    try:
        products = fetch_products()
        load_products(products)
        log_run("bronze", "success", len(products), "raw_products")
    except Exception as e:
        log.error(f"Erro em produtos: {e}")
        log_run("bronze", "error", 0, str(e))

    try:
        users = fetch_users()
        load_users(users)
        log_run("bronze", "success", len(users), "raw_users")
    except Exception as e:
        log.error(f"Erro em usuários: {e}")
        log_run("bronze", "error", 0, str(e))

    try:
        carts = fetch_carts()
        load_carts(carts)
        log_run("bronze", "success", len(carts), "raw_orders")
    except Exception as e:
        log.error(f"Erro em pedidos: {e}")
        log_run("bronze", "error", 0, str(e))

    elapsed = (datetime.now() - start).seconds
    log.info(f"=== Ingestão finalizada em {elapsed}s ===")


if __name__ == "__main__":
    run()