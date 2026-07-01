-- 소스 시스템(이커머스) 스키마. Debezium이 CDC로 캡처할 테이블들.

CREATE TABLE users (
    user_id      SERIAL PRIMARY KEY,
    email        VARCHAR(255) NOT NULL UNIQUE,
    full_name    VARCHAR(255) NOT NULL,
    signup_at    TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    sku          VARCHAR(64) NOT NULL UNIQUE,
    name         VARCHAR(255) NOT NULL,
    category     VARCHAR(100) NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    product_id   INTEGER PRIMARY KEY REFERENCES products(product_id),
    quantity     INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(user_id),
    status       VARCHAR(32) NOT NULL DEFAULT 'CREATED', -- CREATED -> PAID -> SHIPPED -> DELIVERED (or CANCELLED)
    total_amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES orders(order_id),
    product_id    INTEGER NOT NULL REFERENCES products(product_id),
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- Debezium(pgoutput)이 UPDATE/DELETE 시 이전 값까지 캡처할 수 있도록 REPLICA IDENTITY 설정
ALTER TABLE users ALTER COLUMN email SET STATISTICS 100;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Debezium publication (pgoutput 플러그인용)
CREATE PUBLICATION dbz_publication FOR TABLE users, products, inventory, orders, order_items;
