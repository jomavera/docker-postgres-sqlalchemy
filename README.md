# Connection between Postgres database and Python (SQLAlchemy)

This project allows to generate a docker container with a Postgres database. We can make queries using Python to this database in our local environment.

## Technologies

* `Docker` for containerization.

* `Postgres` for datawarehouse.

* `sqlalchemy` for query the database.

* `pandas` for data manipulation.

## Steps

1. Build and start docker container with the database using the following code on the command line:
    
    `docker-compose build && docker-compose up db -d`

    this will make our database available at `localhost:5432`

2. Insert data into tables. From the repository directory run on the command line:

    `python3 insert_tables.py`
    
    this will fill the database tables with the data stored in csv files from the directory `/data`.

    > This python script also fetch todays UF value from [Banco Central de Chile API](https://si3.bcentral.cl/estadisticas/Principal1/Web_Services/index.htm) and inserts it into a table in the database.

3. Run queries to answer questions related with the data stored in the database. From the repository directory run on the command line:

    `python3 query.py`

    this will output in the command line the dataframes with the answers.

### Queries

-	Top 10 stores per transacted amount

This is answered by a sum aggregation of transaction amount grouped by `store_id` from all transactions associated with a store. I considered transactions with all status.

- Top 10 products sold.

Since I detected that for a certain `product_sku` can be associated more than one `product_name`. I decided to use the column `product_sku` as unique identifier. I did a sum aggregation from all transactions grouped by `product_sku`. I considered transactions with all status.

- Average transacted amount per store typology and country.

This can be calculated by a average aggregation of transaction amount grouped by `typology` and `country` of the store associate with the transaction. I considered transactions with all status.

- Percentage of transactions per device type.

I made a count of all transactions and the count of transactions for each device type. With these values I calculated the percentage per device. I considered transactions with all status.

- Average time for a store to perform its 5 first transactions.

Here I considered that the time to its 5 first transactions is the time it takes to a store in each day to have 5 transactions. Here I considered for each store and each day the 5 earliest transactions. Then calculate the max time of the 5 earliest transactions for each store and day. Finally, I calculate the average time (in hours) grouped by store id. Here I considered transactions with all status.
