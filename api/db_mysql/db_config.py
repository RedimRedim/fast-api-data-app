import aiomysql
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from utils.dataframe import DataFrameUtils

logging.basicConfig(level=logging.INFO)

load_dotenv()


class MysqlDatabaseConnection:
    _pool = None

    def __init__(self) -> None:
        self.host = os.getenv("MYSQL_DB_HOST")
        self.database = os.getenv("MYSQL_DB_DATABASE")
        self.user = os.getenv("MYSQL_DB_USER")
        self.password = os.getenv("MYSQL_DB_PASSWORD")
        self.port = int(os.getenv("MYSQL_DB_PORT"))
        self.data_path = os.path.join(os.path.dirname(__file__), "../data")

    async def initialize(self):
        if MysqlDatabaseConnection._pool is None:
            try:
                MysqlDatabaseConnection._pool = await aiomysql.create_pool(
                    host=self.host,
                    db=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    minsize=1,  # Minimum number of connections in the pool
                    maxsize=5,  # Maximum number of connections in the pool
                )
                print("Connected to MYSQL database using connection pool")
            except (Exception, aiomysql.Error) as error:
                print(f"Error connecting to MYSQL database: {error}")
                raise error

    async def execute_and_save_query(self, query, filename) -> None:
        async with MysqlDatabaseConnection._pool_acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute(query)
                    result = await cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    result_df = pd.DataFrame(result, columns=columns)

                    # Save DataFrame to CSV file
                    DataFrameUtils.save_to_csv(result_df, self.data_path, filename)
                    logging.info(f"DataFrame saved to CSV file {self.data_path}")
                except (Exception, aiomysql.Error) as error:
                    print(f"Error executing query: {error}, Connection error")
                    raise error

    async def execute_query_path(self, filename):
        await self.initialize()

        query_path = os.path.join(
            os.path.dirname(__file__), f"../services/query/mysql/refresh_{filename}.sql"
        )

        if not os.path.exists(query_path):
            raise error("Query path does not exist")

        with open(query_path, "r") as file:
            query = file.read()

        async with self._pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    print("running query")
                    await cursor.execute(query)

                    # Fetch the result of the SELECT query
                    result = await cursor.fetchall()

                    # Fetch column names from the query result
                    await cursor.execute(f"SELECT * FROM `mv`.`{filename}` ")
                    result2 = await cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    result_df = pd.DataFrame(result2, columns=columns)

                    # Save DataFrame to CSV file
                    DataFrameUtils.save_to_csv(result_df, self.data_path, filename)
                    logging.info(f"DataFrame saved to CSV file {self.data_path}")

                    await connection.commit()
                except (Exception, aiomysql.Error) as error:
                    await connection.rollback()
                    print(f"Error executing query: {error}, Connection error")
                    raise error

    async def close(self):
        try:
            if self._pool:
                self._pool.close()
                await self._pool.wait_closed()
                print("Connection pool has been closed")
        except Exception as error:
            print(f"Error while closing the connection or cursor: {error}")
            raise error


mysql_db_instance = MysqlDatabaseConnection()
