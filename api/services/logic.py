from db.db_config import pg_db_instance
from db_mysql.db_config import mysql_db_instance
from fastapi import HTTPException
from typing import List, Dict
import os
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)


class ApiLogic:
    def __init__(self):
        self.json_file_path = os.path.join(
            os.path.dirname(__file__), "../data/file_metadata.json"
        )

    def get_filenames_details(self, role) -> List[Dict]:
        if not os.path.exists(self.json_file_path):
            raise HTTPException(
                status_code=404, detail=f"File not found, check the json path"
            )

        with open(self.json_file_path, "r") as file:
            details = json.load(file)

        if role == "admin":
            return details
        elif role == "A":
            return [file for file in details if file["role"] == "A"]
        elif role == "B":
            return [file for file in details if file["role"] == "B"]
        else:
            raise HTTPException(status_code=401, detail="Unauthorized user")

    async def update_file(self, database, filename) -> None:
        """Run query -> Save dataframe into csv file in data folder"""
        if database == "MY":
            await mysql_db_instance.execute_query_path(filename=filename)
        elif database == "PG":
            await pg_db_instance.execute_query_path(filename=filename)

        with open(self.json_file_path, "r") as file:
            details = json.load(file)

        for file_detail in details:
            if filename in file_detail["fileName"]:
                file_detail["updatedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                break

        # save file back into json file
        with open(self.json_file_path, "w") as file:
            json.dump(details, file, indent=4)

    async def update_mysql(self, filename) -> None:
        """re-run query and download to folder data"""
        try:
            await mysql_db_instance.execute_query_path(filename=filename)
        except Exception as error:
            raise HTTPException(
                status_code=500, detail=f"Error executing query: {error}"
            )


ApiLogicInstance = ApiLogic()
