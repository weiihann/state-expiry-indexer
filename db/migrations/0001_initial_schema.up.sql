-- 20-byte Ethereum address
CREATE DOMAIN eth_address AS BYTEA
  CHECK (octet_length(VALUE) = 20);

-- 32-byte storage slot key
CREATE DOMAIN eth_slotkey AS BYTEA
  CHECK (octet_length(VALUE) = 32);

CREATE TABLE accounts_current (
  address           eth_address   NOT NULL,
  last_access_block BIGINT        NOT NULL,
  CONSTRAINT pk_accounts_current PRIMARY KEY (address)
)
PARTITION BY HASH (address);

CREATE TABLE storage_current (
  address           eth_address   NOT NULL,
  slot_key          eth_slotkey   NOT NULL,
  last_access_block BIGINT        NOT NULL,
  CONSTRAINT pk_storage_current PRIMARY KEY (address, slot_key)
)
PARTITION BY HASH (address); 