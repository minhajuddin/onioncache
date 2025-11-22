CREATE DATABASE onioncache_testdb;

\c onioncache_testdb

CREATE TABLE pricing_plans (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO pricing_plans (name, price) VALUES
  ('Free Tier', 0.00),
  ('Starter', 9.99),
  ('Professional', 29.99),
  ('Business', 79.99),
  ('Enterprise', 199.99),
  ('Premium', 49.99),
  ('Team', 99.99),
  ('Ultimate', 299.99),
  ('Developer', 19.99),
  ('Corporate', 499.99);
