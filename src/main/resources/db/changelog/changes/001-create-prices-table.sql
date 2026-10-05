--liquibase formatted sql

--changeset evgeny:001-create-prices-table
CREATE TABLE prices (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_prices_symbol_created_at ON prices(symbol, created_at DESC);