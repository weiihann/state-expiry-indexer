CREATE TABLE metadata (
    key VARCHAR(255) PRIMARY KEY,
    value VARCHAR(255) NOT NULL
);

INSERT INTO metadata (key, value) VALUES ('last_indexed_block', '0'); 