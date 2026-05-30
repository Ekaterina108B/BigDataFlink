# Лабораторная работа №3: Потоковая обработка данных (Flink Streaming)

## Цель

Построить потоковый ETL-пайплайн:
1. **Producer** — читает CSV-файлы и отправляет каждую строку как JSON-сообщение в Kafka
2. **Flink Job** — читает из Kafka, трансформирует данные в схему "звезда" и записывает в PostgreSQL

## Стек технологий

- PostgreSQL 15
- Apache Kafka 3.7 (KRaft mode, без ZooKeeper)
- Apache Flink 1.18 (PyFlink, Table API)
- Docker / Docker Compose
- Python 3.11

## Схема данных (звезда)

```
dim_customer (customer_id PK)
dim_seller (seller_id PK)
dim_product (product_id PK, includes supplier_name, supplier_city)
dim_store (store_name + store_city composite PK)
dim_supplier (supplier_name + supplier_city composite PK)
fact_sales (sale_id PK, references dimensions via natural/business keys)
```

## Запуск

```bash
cd 3/
docker compose up --build
```

Процесс запуска:
1. PostgreSQL создаёт таблицы из `init/init.sql`
2. Kafka стартует в KRaft режиме
3. Producer читает CSV и отправляет ~10000 JSON-сообщений в топик `sales_raw`
4. Flink JobManager и TaskManager запускаются
5. Flink Job автоматически отправляется на выполнение (~30 сек задержка для ожидания готовности кластера)

## Проверка результатов

### Через Flink WebUI

Откройте http://localhost:8081 — job должен быть в статусе RUNNING (это нормально для streaming).

### Через SQL (psql или DBeaver)

```bash
docker exec -it lab3_postgres psql -U admin -d sales_db
```

```sql
-- Общее количество фактов (~10000)
SELECT COUNT(*) FROM fact_sales;

-- Количество уникальных клиентов
SELECT COUNT(*) FROM dim_customer;

-- Количество уникальных продавцов
SELECT COUNT(*) FROM dim_seller;

-- Количество уникальных товаров
SELECT COUNT(*) FROM dim_product;

-- Количество уникальных магазинов
SELECT COUNT(*) FROM dim_store;

-- Количество уникальных поставщиков
SELECT COUNT(*) FROM dim_supplier;

-- Пример join-запроса
SELECT
    f.sale_id,
    c.first_name || ' ' || c.last_name AS customer,
    p.name AS product,
    f.sale_quantity,
    f.sale_total_price,
    f.sale_date
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
JOIN dim_product p ON p.product_id = f.product_id
LIMIT 10;
```

## Демонстрация потоковости

Producer можно перезапустить, и новые данные появятся в PostgreSQL:

```bash
docker compose up -d --force-recreate producer
```

## Остановка

```bash
docker compose down -v
```

## Структура проекта

```
3/
├── docker-compose.yml         # Оркестрация всех сервисов
├── INSTRUCTION.md             # Данный файл
├── data/                      # Исходные CSV-файлы (10 файлов по 1000 строк)
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   ├── ...
│   └── MOCK_DATA (9).csv
├── init/
│   └── init.sql               # DDL — создание таблиц звезды
├── producer/
│   ├── Dockerfile
│   ├── producer.py            # CSV → Kafka JSON
│   └── requirements.txt
└── flink/
    ├── Dockerfile             # Flink + Python + коннекторы
    ├── flink_job.py           # Kafka → PostgreSQL (PyFlink Table API)
    └── requirements.txt
```

## Примечания

- Streaming job не завершается — он продолжает слушать Kafka. Статус RUNNING — это ожидаемое поведение.
- `dim_store` и `dim_supplier` используют составной первичный ключ (name + city), т.к. в исходных данных нет числового идентификатора для этих сущностей.
- В `fact_sales` ссылка на магазин хранится как `(store_name, store_city)` — текстовый составной ключ.
- FK constraints не используются в streaming-режиме, т.к. факты и измерения записываются параллельно.
