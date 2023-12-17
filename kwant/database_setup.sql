CREATE TABLE exchanges (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cryptocurrencies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    symbol VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE market_pairs (
    id SERIAL PRIMARY KEY,
    base_currency_id INTEGER NOT NULL,
    quote_currency_id INTEGER NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange_id INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (base_currency_id) REFERENCES cryptocurrencies (id),
    FOREIGN KEY (quote_currency_id) REFERENCES cryptocurrencies (id),
    FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
);

CREATE TYPE timeframe_enum AS ENUM ('1m', '5m', '15m', '30m', '1h', '4h', '12h', '1d', '1w', '1M');

CREATE TABLE ohlcv (
    id SERIAL PRIMARY KEY,
    market_pair_id INTEGER NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    timeframe timeframe_enum NOT NULL,
    open_price DECIMAL(20, 15) NOT NULL,
    high_price DECIMAL(20, 15) NOT NULL,
    low_price DECIMAL(20, 15) NOT NULL,
    close_price DECIMAL(20, 15) NOT NULL,
    volume DECIMAL(20, 15) NOT NULL,
    open_time TIMESTAMP NOT NULL,
    close_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_pair_id) REFERENCES market_pairs (id)
);
