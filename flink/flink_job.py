import os
from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sales_raw")
PG_DB = os.environ.get("POSTGRES_DB", "sales_db")
PG_USER = os.environ.get("POSTGRES_USER", "admin")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")
PG_URL = f"jdbc:postgresql://postgres:5432/{PG_DB}"


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("parallelism.default", "1")
    
    t_env.get_config().set("table.exec.mini-batch.enabled", "false")
    t_env.get_config().set("pipeline.operator-chaining", "false")

    # Kafka source — all fields as STRING
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `id` STRING,
            `customer_first_name` STRING,
            `customer_last_name` STRING,
            `customer_age` STRING,
            `customer_email` STRING,
            `customer_country` STRING,
            `customer_postal_code` STRING,
            `customer_pet_type` STRING,
            `customer_pet_name` STRING,
            `customer_pet_breed` STRING,
            `seller_first_name` STRING,
            `seller_last_name` STRING,
            `seller_email` STRING,
            `seller_country` STRING,
            `seller_postal_code` STRING,
            `product_name` STRING,
            `product_category` STRING,
            `product_price` STRING,
            `product_quantity` STRING,
            `sale_date` STRING,
            `sale_customer_id` STRING,
            `sale_seller_id` STRING,
            `sale_product_id` STRING,
            `sale_quantity` STRING,
            `sale_total_price` STRING,
            `store_name` STRING,
            `store_location` STRING,
            `store_city` STRING,
            `store_state` STRING,
            `store_country` STRING,
            `store_phone` STRING,
            `store_email` STRING,
            `pet_category` STRING,
            `product_weight` STRING,
            `product_color` STRING,
            `product_size` STRING,
            `product_brand` STRING,
            `product_material` STRING,
            `product_description` STRING,
            `product_rating` STRING,
            `product_reviews` STRING,
            `product_release_date` STRING,
            `product_expiry_date` STRING,
            `supplier_name` STRING,
            `supplier_contact` STRING,
            `supplier_email` STRING,
            `supplier_phone` STRING,
            `supplier_address` STRING,
            `supplier_city` STRING,
            `supplier_country` STRING,
            `global_sale_id` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-sales-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Enriched view with proper type casts
    t_env.execute_sql("""
        CREATE VIEW enriched AS
        SELECT
            CAST(`global_sale_id` AS INT) AS sale_id,
            CAST(NULLIF(`sale_customer_id`, '') AS INT) AS customer_id,
            CAST(NULLIF(`sale_seller_id`, '') AS INT) AS seller_id,
            CAST(NULLIF(`sale_product_id`, '') AS INT) AS product_id,
            `customer_first_name`,
            `customer_last_name`,
            CAST(NULLIF(`customer_age`, '') AS INT) AS customer_age,
            `customer_email`,
            `customer_country`,
            `customer_postal_code`,
            `customer_pet_type`,
            `customer_pet_name`,
            `customer_pet_breed`,
            `seller_first_name`,
            `seller_last_name`,
            `seller_email`,
            `seller_country`,
            `seller_postal_code`,
            `product_name`,
            `product_category`,
            CAST(NULLIF(`product_price`, '') AS DECIMAL(10,2)) AS product_price,
            CAST(NULLIF(`product_quantity`, '') AS INT) AS product_quantity,
            CAST(NULLIF(`product_weight`, '') AS DECIMAL(10,2)) AS product_weight,
            `product_color`,
            `product_size`,
            `product_brand`,
            `product_material`,
            `product_description`,
            CAST(NULLIF(`product_rating`, '') AS DECIMAL(3,1)) AS product_rating,
            CAST(NULLIF(`product_reviews`, '') AS INT) AS product_reviews,
            CASE
                WHEN `product_release_date` IS NOT NULL AND `product_release_date` <> ''
                THEN TO_DATE(`product_release_date`, 'M/d/yyyy')
                ELSE NULL
            END AS product_release_date,
            CASE
                WHEN `product_expiry_date` IS NOT NULL AND `product_expiry_date` <> ''
                THEN TO_DATE(`product_expiry_date`, 'M/d/yyyy')
                ELSE NULL
            END AS product_expiry_date,
            `supplier_name`,
            `supplier_contact`,
            `supplier_email`,
            `supplier_phone`,
            `supplier_address`,
            `supplier_city`,
            `supplier_country`,
            `store_name`,
            `store_location`,
            `store_city`,
            `store_state`,
            `store_country`,
            `store_phone`,
            `store_email`,
            CASE
                WHEN `sale_date` IS NOT NULL AND `sale_date` <> ''
                THEN TO_DATE(`sale_date`, 'M/d/yyyy')
                ELSE NULL
            END AS sale_date,
            CAST(NULLIF(`sale_quantity`, '') AS INT) AS sale_quantity,
            CAST(NULLIF(`sale_total_price`, '') AS DECIMAL(12,2)) AS sale_total_price
        FROM kafka_source
    """)

    # --- JDBC sink tables ---

    t_env.execute_sql(f"""
        CREATE TABLE dim_customer_sink (
            customer_id INT,
            first_name STRING,
            last_name STRING,
            age INT,
            email STRING,
            country STRING,
            postal_code STRING,
            pet_type STRING,
            pet_name STRING,
            pet_breed STRING,
            PRIMARY KEY (customer_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_customer',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_seller_sink (
            seller_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            country STRING,
            postal_code STRING,
            PRIMARY KEY (seller_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_seller',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_product_sink (
            product_id INT,
            name STRING,
            category STRING,
            price DECIMAL(10,2),
            quantity INT,
            weight DECIMAL(10,2),
            color STRING,
            size STRING,
            brand STRING,
            material STRING,
            description STRING,
            rating DECIMAL(3,1),
            reviews INT,
            release_date DATE,
            expiry_date DATE,
            supplier_name STRING,
            supplier_city STRING,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_product',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_store_sink (
            store_name STRING,
            store_city STRING,
            store_location STRING,
            store_state STRING,
            store_country STRING,
            store_phone STRING,
            store_email STRING,
            PRIMARY KEY (store_name, store_city) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_store',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dim_supplier_sink (
            supplier_name STRING,
            supplier_city STRING,
            supplier_contact STRING,
            supplier_email STRING,
            supplier_phone STRING,
            supplier_address STRING,
            supplier_country STRING,
            PRIMARY KEY (supplier_name, supplier_city) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'dim_supplier',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fact_sales_sink (
            sale_id INT,
            customer_id INT,
            seller_id INT,
            product_id INT,
            store_name STRING,
            store_city STRING,
            sale_date DATE,
            sale_quantity INT,
            sale_total_price DECIMAL(12,2),
            PRIMARY KEY (sale_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'fact_sales',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '2s'
        )
    """)

    # --- Statement set for parallel inserts ---
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO dim_customer_sink
        SELECT
            customer_id,
            customer_first_name,
            customer_last_name,
            customer_age,
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed
        FROM enriched
        WHERE customer_id IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_seller_sink
        SELECT
            seller_id,
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code
        FROM enriched
        WHERE seller_id IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_product_sink
        SELECT
            product_id,
            product_name,
            product_category,
            product_price,
            product_quantity,
            product_weight,
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            product_rating,
            product_reviews,
            product_release_date,
            product_expiry_date,
            supplier_name,
            supplier_city
        FROM enriched
        WHERE product_id IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_store_sink
        SELECT
            store_name,
            store_city,
            store_location,
            store_state,
            store_country,
            store_phone,
            store_email
        FROM enriched
        WHERE store_name IS NOT NULL AND store_city IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_supplier_sink
        SELECT
            supplier_name,
            supplier_city,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_country
        FROM enriched
        WHERE supplier_name IS NOT NULL AND supplier_city IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO fact_sales_sink
        SELECT
            sale_id,
            customer_id,
            seller_id,
            product_id,
            store_name,
            store_city,
            sale_date,
            sale_quantity,
            sale_total_price
        FROM enriched
        WHERE sale_id IS NOT NULL
    """)

    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
