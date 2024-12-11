import psycopg2
import pandas as pd
from pandas import DataFrame
import os
import logging
from dotenv import load_dotenv
from utils.dataframe import DataFrameUtils

logging.basicConfig(level=logging.INFO)

load_dotenv()


class DatabaseConnection:
    def __init__(self, host, database, user, password, port) -> None:
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None
        self.data_path = os.path.join(os.path.dirname(__file__), "../data")
        # Initialize connection
        self.initialize()

    def initialize(self):
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                )
                self.cursor = self.connection.cursor()
                print("Connected to database")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Error connecting to PostgreSQL database: {error}")

    def execute_and_save_query(self, query, filename) -> None:
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]  # Get column names
            result_df = pd.DataFrame(result, columns=columns)

            # Save DataFrame to CSV file
            DataFrameUtils.save_to_csv(result_df, self.data_path, filename)
            logging.info(f"DataFrame saved to CSV file {self.data_path}")
        except (Exception, psycopg2.DatabaseError) as error:
            self.cursor.connection.rollback()
            print(f"Error executing query: {error}, Connection error")
            raise error

    async def close(self):
        """Close the database connection and cursor"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.cursor = None
        self.connection = None
