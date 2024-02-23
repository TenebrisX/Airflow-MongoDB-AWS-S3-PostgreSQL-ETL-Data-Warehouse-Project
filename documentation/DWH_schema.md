# Data Warehouse Documentation

## STG Layer (AS IS)
![Data Warehouse STG LAYER Architecture](https://example.com/images/data_warehouse_architecture.png)

### Postgres Database source:

#### Airflow DAG
- [stg_bonus_system_dag](link-to-repo/stg/dags/stg_bonus_system_dag.sql)

#### SQL scripts
- [stg.bonussystem_events Table](link-to-repo/stg/bonussystem_events.sql)
- [stg.bonussystem_ranks Table](link-to-repo/stg/bonussystem_ranks.sql)
- [stg.bonussystem_users Table](link-to-repo/stg/bonussystem_users.sql)

### API AWS S3 source:

#### Airflow DAG
- [stg_delivery_system_dag DAG](link-to-repo/stg/dags/stg_delivery_system_dag.sql)

#### SQL scripts
- [stg.deliverysystem_couriers Table](link-to-repo/stg/deliverysystem_couriers.sql)
- [stg.deliverysystem_deliveries Table](link-to-repo/stg/deliverysystem_deliveries.sql)
- [stg.deliverysystem_restaurants Table](link-to-repo/stg/deliverysystem_restaurants.sql)

### MongoDB source:

#### Airflow DAG
- [stg_order_system_dag DAG](link-to-repo/stg/dags/stg_order_system_dag.sql)

#### SQL scripts
- [stg.ordersystem_orders Table](link-to-repo/stg/ordersystem_orders.sql)
- [stg.ordersystem_restaurants Table](link-to-repo/stg/ordersystem_restaurants.sql)
- [stg.ordersystem_users Table](link-to-repo/stg/ordersystem_users.sql)

### Workflow settings
- [stg.srv_wf_settings Table](link-to-repo/stg/srv_wf_settings.sql)

## DDS Layer (Snowflake)
![Data Warehouse STG LAYER Architecture](https://example.com/images/data_warehouse_architecture.png)

#### Airflow DAG
- [dds DAG](link-to-repo/stg/dags/dds_dag.py)

#### SQL scripts
### Dimentions:
- [dds.dm_users Table](link-to-repo/dds/dm_users.sql)
- [dds.dm_restaurants Table](link-to-repo/dds/dm_restaurants.sql)
- [dds.c_couriers Table](link-to-repo/dds/c_couriers.sql)
- [dds.dm_products Table](link-to-repo/dds/dm_products.sql)
- [dds.dm_timestamps Table](link-to-repo/dds/dm_timestamps.sql)
- [dds.c_deliveries Table](link-to-repo/dds/c_deliveries.sql)
- [dds.dm_orders Table](link-to-repo/dds/dm_orders.sql)

### Facts table
- [dds.fct_product_sales Table](link-to-repo/dds/fct_product_sales.sql)

### Workflow settings
- [dds.srv_wf_settings Table](link-to-repo/dds/srv_wf_settings.sql)

## CDM Layer
![Data Warehouse STG LAYER Architecture](https://example.com/images/data_warehouse_architecture.png)

#### Airflow DAG
- [cdm DAG](link-to-repo/stg/dags/cdm_dag.py)

#### SQL scripts
### Data Marts:
- [cdm.dm_courier_ledger Table](link-to-repo/cdm/dm_courier_ledger.sql)
- [cdm.dm_settlement_report Table](link-to-repo/cdm/dm_settlement_report.sql)

### Workflow settings
- [cdm.srv_wf_settings Table](link-to-repo/cdm/srv_wf_settings.sql)
