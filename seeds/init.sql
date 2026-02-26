CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    signup_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    country_code CHAR(2) NOT NULL,
    subscription_tier VARCHAR(50) DEFAULT 'free',
    lifetime_value NUMERIC(10, 2) DEFAULT 0.00
);

CREATE INDEX idx_users_country_code ON users(country_code);
CREATE INDEX idx_users_subscription_tier ON users(subscription_tier);
CREATE INDEX idx_users_lifetime_value ON users(lifetime_value);

INSERT INTO users (name, email, country_code, subscription_tier, lifetime_value)
SELECT 
    'User ' || gs,
    'user' || gs || '@example.com',
    (ARRAY['US','IN','UK','CA','AU'])[floor(random()*5+1)],
    (ARRAY['free','premium','enterprise'])[floor(random()*3+1)],
    round((random()*1000)::numeric,2)
FROM generate_series(1, 10000000) AS gs;