from db.db_config import pg_db_instance
from db_mysql.db_config import mysql_db_instance
from fastapi import HTTPException
from typing import List, Dict
import os
import json
from datetime import datetime, timedelta
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

    def check_get_update(self, filename, method) -> datetime:
        with open(self.json_file_path, "r") as file:
            details = json.load(file)

        if method == "get_update":

            for file_detail in details:
                if filename in file_detail["fileName"].lower():
                    print(
                        file_detail["fileName"],
                        datetime.strptime(
                            file_detail["updatedAt"], "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                    return datetime.strptime(
                        file_detail["updatedAt"], "%Y-%m-%d %H:%M:%S"
                    )
            return None
        elif method == "put_update":
            for file_detail in details:
                if filename in file_detail["fileName"].lower():
                    file_detail["updatedAt"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    break

            # save file back into json file
            with open(self.json_file_path, "w") as file:
                json.dump(details, file, indent=4)

            return None

    async def update_file(self, database, filename) -> None:
        last_updated_time = self.check_get_update(filename, "get_update")
        if last_updated_time and last_updated_time > (
            datetime.now() - timedelta(minutes=3)
        ):
            print("error updateding")
            raise HTTPException(
                status_code=400,
                detail=f"File {filename} was updated less than 3 minutes ago",
            )

        """Run query -> Save dataframe into csv file in data folder"""
        if database == "MY":
            await mysql_db_instance.execute_query_path(filename=filename)
        elif database == "PG":
            await pg_db_instance.execute_query_path(filename=filename)

        self.check_get_update(filename, "put_update")

    async def update_mysql(self, filename) -> None:
        """re-run query and download to folder data"""
        try:
            await mysql_db_instance.execute_query_path(filename=filename)
        except Exception as error:
            raise HTTPException(
                status_code=500, detail=f"Error executing query: {error}"
            )


ApiLogicInstance = ApiLogic()
