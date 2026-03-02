# ecommerce-dashboard

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

```
npm install -g @go-task/cli
```

```
psql -U postgres


-- Postgres replication enable
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Administrator mode
net stop postgresql-x64-17
net start postgresql-x64-17
```

```
-- Debezium user permissions
ALTER USER postgres REPLICATION;
```

ecommerce_db