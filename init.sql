-- REQUIERE THE VECTOR EXTENSION FOR POSTGRES!
-- https://github.com/pgvector/pgvector
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE item_origin (
    id serial primary key,
    file_name varchar(50) unique
);

CREATE TABLE vectorized_item (
    id SERIAL PRIMARY KEY,
    content TEXT,
    embedding VECTOR(384),
    origin_id int,
    foreign key(origin_id) references item_origin(id)
);
