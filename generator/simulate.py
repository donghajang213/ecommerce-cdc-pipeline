"""주문 라이프사이클을 지속적으로 시뮬레이션해서 Debezium이 캡처할 CDC 이벤트를 발생시킨다.

- 새 주문 생성 (orders + order_items INSERT, inventory UPDATE)
- 기존 주문 상태 전이 (CREATED -> PAID -> SHIPPED -> DELIVERED)
- 가끔 상품 가격 변경 / 재고 재입고 (products/inventory UPDATE)
"""
import random
import time

from db import get_connection

STATUS_FLOW = ["CREATED", "PAID", "SHIPPED", "DELIVERED"]
SLEEP_RANGE = (1.0, 3.0)


def create_order(cur):
    cur.execute("SELECT user_id FROM users ORDER BY random() LIMIT 1")
    user_id = cur.fetchone()[0]

    cur.execute("INSERT INTO orders (user_id, status) VALUES (%s, 'CREATED') RETURNING order_id", (user_id,))
    order_id = cur.fetchone()[0]

    cur.execute(
        "SELECT product_id, price FROM products ORDER BY random() LIMIT %s",
        (random.randint(1, 4),),
    )
    items = cur.fetchall()

    total = 0
    for product_id, price in items:
        qty = random.randint(1, 3)
        cur.execute(
            """
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_id, qty, price),
        )
        cur.execute(
            "UPDATE inventory SET quantity = GREATEST(quantity - %s, 0), updated_at = now() WHERE product_id = %s",
            (qty, product_id),
        )
        total += float(price) * qty

    cur.execute("UPDATE orders SET total_amount = %s, updated_at = now() WHERE order_id = %s", (total, order_id))
    print(f"[order] created order_id={order_id} user_id={user_id} items={len(items)} total={total:.2f}")


def advance_order_status(cur):
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status != 'DELIVERED' AND status != 'CANCELLED' "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, status = row
    idx = STATUS_FLOW.index(status)
    if idx + 1 >= len(STATUS_FLOW):
        return
    next_status = STATUS_FLOW[idx + 1]
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
        (next_status, order_id),
    )
    print(f"[order] order_id={order_id} status {status} -> {next_status}")


def restock_or_reprice(cur):
    cur.execute("SELECT product_id FROM products ORDER BY random() LIMIT 1")
    product_id = cur.fetchone()[0]
    if random.random() < 0.5:
        qty_add = random.randint(10, 100)
        cur.execute(
            "UPDATE inventory SET quantity = quantity + %s, updated_at = now() WHERE product_id = %s",
            (qty_add, product_id),
        )
        print(f"[inventory] restocked product_id={product_id} +{qty_add}")
    else:
        cur.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
        price = float(cur.fetchone()[0])
        new_price = round(price * random.uniform(0.9, 1.1), 2)
        cur.execute(
            "UPDATE products SET price = %s, updated_at = now() WHERE product_id = %s",
            (new_price, product_id),
        )
        print(f"[product] price change product_id={product_id} {price:.2f} -> {new_price:.2f}")


def main():
    conn = get_connection()
    print("Starting order lifecycle simulator...")
    while True:
        with conn.cursor() as cur:
            action = random.choices(
                [create_order, advance_order_status, restock_or_reprice],
                weights=[0.5, 0.35, 0.15],
            )[0]
            try:
                action(cur)
            except Exception as err:  # noqa: BLE001
                print(f"[error] {action.__name__} failed: {err}")
        time.sleep(random.uniform(*SLEEP_RANGE))


if __name__ == "__main__":
    main()
