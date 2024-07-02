from sqlalchemy import create_engine, insert, text
import sqlalchemy
import pandas as pd

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/DB")
conn = engine.connect()

top10_stores_by_amount = \
"""select
    st.id as store_id,
    st.name as store_name,
    sum(trx.amount) as amount
    from transaction trx
    inner join device dv on trx.device_id = dv.id
    inner join store st on dv.store_id = st.id
    group by st.id, st.name
    order by amount desc limit 10;
"""

top10_products_sold = \
"""select
    trx.product_sku,
    sum(trx.amount) as amount
    from transaction trx
    group by trx.product_sku
    order by amount desc limit 10;
"""

avg_trx_amnt_per_typ_country = \
"""select
    st.typology,
    st.country,
    avg(trx.amount) as amount
    from transaction trx
    inner join device dv on trx.device_id = dv.id
    inner join store st on dv.store_id = st.id
    group by st.typology, st.country;
"""

prc_trx_per_device = \
"""select
    dv.type,
    count(*) as cnt,
    count(*) * 1.0 / sum(count(*)) over () as percentage
    from transaction trx
    inner join device dv on trx.device_id = dv.id
    group by type;
"""

avg_time_5_trx = \
"""
   select
   store_id,
   store_name,
   avg(fifth_trx_time)/60 as avg_hours_to_5_trx
   from
   (select
    id as store_id,
    name as store_name,
    trx_date,
    MAX(EXTRACT(EPOCH FROM trx_time ))/60 fifth_trx_time
   from (
    select
        ROW_NUMBER() OVER (PARTITION BY id, name, trx_date ORDER BY trx_time) AS r,
        sub.*
    from
        (select
            st.id,
            st.name,
            trx.happened_at::time as trx_time,
            trx.happened_at::date as trx_date
            from transaction trx
            inner join device dv on trx.device_id = dv.id
            inner join store st on dv.store_id = st.id) as sub
    ) x
    where x.r <=5
    group by  id, name, trx_date)
    group by store_id, store_name;
"""

print("# -- # -- # -- # Top 10 stores per transacted amount. # -- # -- # -- #")
query=text(top10_stores_by_amount)
cursor = conn.execute(query)
df_1 = pd.DataFrame(cursor.fetchall())
print(df_1, "\n")

print("# -- # -- # -- # Top 10 products sold. # -- # -- # -- #")
query=text(top10_products_sold)
cursor = conn.execute(query)
df_2 = pd.DataFrame(cursor.fetchall())
print(df_2, "\n")

print("# -- # -- # -- # Average transacted amount per store typology and country. # -- # -- # -- #")
query = text(avg_trx_amnt_per_typ_country)
cursor = conn.execute(query)
df_3 = pd.DataFrame(cursor.fetchall())
df_3['amount'] = df_3['amount'].map(lambda x:round(x,3))
print(df_3, "\n")

print("# -- # -- # -- # Percentage of transactions per device type. # -- # -- # -- #")
query = text(prc_trx_per_device)
cursor = conn.execute(query)
df_4 = pd.DataFrame(cursor.fetchall())
df_4['percentage'] = df_4['percentage'].map(lambda x: round(x,5))
df_4 = df_4.drop(columns=['cnt'])
print(df_4, "\n")

print("# -- # -- # -- # Average time for a store to perform its 5 first transactions. # -- # -- # -- #")
query = text(avg_time_5_trx)
cursor = conn.execute(query)
df_5 = pd.DataFrame(cursor.fetchall())
df_5['avg_hours_to_5_trx'] = df_5['avg_hours_to_5_trx'].map(lambda x: round(x, 3))
print(df_5, "\n")

engine.dispose()
