-- Enable pgvector (already supported in ankane/pgvector)
CREATE EXTENSION IF NOT EXISTS vector;

-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- Orders table
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    user_id UUID,
    amount DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_user
    FOREIGN KEY (user_id)
    REFERENCES users(id)
);

-- Products table
CREATE TABLE products (
    id UUID PRIMARY KEY,
    name TEXT,
    price DECIMAL
);

-- Order Items (many-to-many)
CREATE TABLE order_items (
    id UUID PRIMARY KEY,
    order_id UUID,
    product_id UUID,
    quantity INT,

    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Sample function
CREATE OR REPLACE FUNCTION calculate_discount(price DECIMAL)
RETURNS DECIMAL AS $$
BEGIN
    RETURN price * 0.9;
END;
$$ LANGUAGE plpgsql;

-- Sample procedure (Postgres 11+)
CREATE OR REPLACE PROCEDURE log_order()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Order logged';
END;
$$;