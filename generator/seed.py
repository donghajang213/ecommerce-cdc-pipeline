"""초기 시드 데이터 생성 (idempotent). 이미 데이터가 있으면 스킵."""
import random

from faker import Faker

from db import get_connection

fake = Faker("ko_KR")

NUM_USERS = 50
NUM_PRODUCTS = 40
CATEGORIES = ["뷰티", "전자기기", "식품", "의류", "생활용품"]


def already_seeded(cur) -> bool:
    cur.execute("SELECT COUNT(*) FROM users")
    return cur.fetchone()[0] > 0


def seed_users(cur):
    for _ in range(NUM_USERS):
        cur.execute(
            "INSERT INTO users (email, full_name) VALUES (%s, %s)",
            (fake.unique.email(), fake.name()),
        )


def seed_products_and_inventory(cur):
    for i in range(NUM_PRODUCTS):
        category = random.choice(CATEGORIES)
        name = f"{fake.word().capitalize()} {category} {i + 1}"
        sku = f"SKU-{i + 1:05d}"
        price = round(random.uniform(3_000, 150_000), 2)
        cur.execute(
            """
            INSERT INTO products (sku, name, category, price)
            VALUES (%s, %s, %s, %s)
            RETURNING product_id
            """,
            (sku, name, category, price),
        )
        product_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO inventory (product_id, quantity) VALUES (%s, %s)",
            (product_id, random.randint(50, 500)),
        )


def main():
    conn = get_connection()
    with conn.cursor() as cur:
        if already_seeded(cur):
            print("Already seeded, skipping.")
            return
        seed_users(cur)
        seed_products_and_inventory(cur)
    print(f"Seeded {NUM_USERS} users and {NUM_PRODUCTS} products.")


if __name__ == "__main__":
    main()
