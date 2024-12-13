import asyncpg
import pandas as pd
from pandas import DataFrame
import os
import logging
from dotenv import load_dotenv
from utils.dataframe import DataFrameUtils

logging.basicConfig(level=logging.INFO)

load_dotenv()


class DatabaseConnection:

    def __init__(self) -> None:
        self.host = os.getenv("DB_HOST")
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = int(os.getenv("DB_PORT"))
        self._pool = None
        self.data_path = os.path.join(os.path.dirname(__file__), "../data")

    async def initialize(self) -> None:
        if self._pool is None:
            try:
                self._pool = await asyncpg.create_pool(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    min_size=1,
                    max_size=5,
                )
                print("Connected to Postgresql database")
            except (Exception, asyncpg.PostgresError) as error:
                print(f"Error connecting to PostgreSQL database: {error}")
                raise error

    async def execute_and_save_query(self, query, filename) -> None:
        """Execute the given query and save the result to a CSV file"""
        await self.initialize()

        async with self.pool.acquire() as connection:
            try:
                print("Executing query")
                result = await connection.fetch(query)
                if result:
                    columns = list(result[0].keys())  # Get column names
                    result_df = pd.DataFrame(result, columns=columns)

                    # Save DataFrame to CSV file
                    DataFrameUtils.save_to_csv(result_df, self.data_path, filename)
                    logging.info(f"DataFrame saved to CSV file {self.data_path}")
            except (Exception, asyncpg.PostgresError) as error:
                print(f"Error executing query: {error}, Connection error")
                raise error

    async def execute_query_path(self, filename):
        await self.initialize()

        query_path = os.path.join(
            os.path.dirname(__file__), f"../services/query/postgres/{filename}.sql"
        )

        if not os.path.exists(query_path):
            raise FileNotFoundError("Query path does not exist")

        with open(query_path, "r") as file:
            query = file.read()

        async with self._pool.acquire() as connection:
            try:
                print("Executing query")
                result = await connection.fetch(query)
                if result:
                    columns = list(result[0].keys())  # Get column names
                    result_df = pd.DataFrame(result, columns=columns)

                    # Save DataFrame to CSV file
                    DataFrameUtils.save_to_csv(result_df, self.data_path, filename)
                    logging.info(f"DataFrame saved to CSV file {self.data_path}")
            except (Exception, asyncpg.PostgresError) as error:
                print(f"Error executing query: {error}, Connection error")
                raise error

    async def close(self):
        """Close the database connection and cursor"""
        try:
            if self._pool:
                await self._pool.close()
        except Exception as error:
            print(f"Error while closing the connection or cursor: {error}")
            raise error


pg_db_instance = DatabaseConnection()
