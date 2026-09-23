-- ============================================================================
-- PostgreSQL → MongoDB Migration Lab
-- Database: migration_lab
-- Tables: customers, products, orders, order_items, payments
-- ============================================================================

-- ─── Customers ─────────────────────────────────────────────────────────────
CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100)  NOT NULL,
    email       VARCHAR(150)  UNIQUE NOT NULL,
    phone       VARCHAR(20),
    created_at  TIMESTAMPTZ   DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   DEFAULT NOW()
);

-- ─── Products ──────────────────────────────────────────────────────────────
CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200)  NOT NULL,
    category    VARCHAR(50),
    price       DECIMAL(10,2) NOT NULL,
    stock       INTEGER       DEFAULT 0,
    created_at  TIMESTAMPTZ   DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   DEFAULT NOW()
);

-- ─── Orders ────────────────────────────────────────────────────────────────
CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INTEGER       NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    status        VARCHAR(20)   DEFAULT 'PENDING',
    total_amount  DECIMAL(12,2) DEFAULT 0,
    created_at    TIMESTAMPTZ   DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   DEFAULT NOW()
);

-- ─── Order Items ───────────────────────────────────────────────────────────
CREATE TABLE order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INTEGER       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id  INTEGER       NOT NULL REFERENCES products(id),
    quantity    INTEGER       NOT NULL,
    unit_price  DECIMAL(10,2) NOT NULL,
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

-- ─── Payments ──────────────────────────────────────────────────────────────
CREATE TABLE payments (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER       NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    payment_method  VARCHAR(20)   NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    status          VARCHAR(20)   DEFAULT 'PENDING',
    transaction_ref VARCHAR(100),
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   DEFAULT NOW()
);

-- ─── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX idx_orders_customer_id    ON orders(customer_id);
CREATE INDEX idx_orders_status         ON orders(status);
CREATE INDEX idx_orders_updated_at     ON orders(updated_at);
CREATE INDEX idx_order_items_order_id  ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_payments_order_id     ON payments(order_id);
CREATE INDEX idx_payments_status       ON payments(status);
CREATE INDEX idx_customers_updated_at  ON customers(updated_at);
CREATE INDEX idx_products_category     ON products(category);

-- ─── Trigger for updated_at ────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ─── Publication for Debezium CDC ──────────────────────────────────────────
-- Debezium reads changes via this publication using pgoutput plugin.
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
