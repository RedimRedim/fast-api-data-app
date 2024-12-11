from db.setup import db
from fastapi import HTTPException
import psycopg2
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

    def get_filename_query(self, filename):
        files_query = {
            "Shinkansen_Station": """ SELECT * FROM "test"."shinkansen_station" """,
            "Tokyo": """ SELECT * FROM "test"."shinkansen_station" WHERE "Prefecture" = 'Tokyo' """,
        }

        if filename in files_query:
            return files_query[filename]

    def get_filenames_details(self) -> List[Dict]:

        if not os.path.exists(self.json_file_path):
            raise HTTPException(
                status_code=404, detail=f"File not found, check the json path"
            )

        with open(self.json_file_path, "r") as file:
            details = json.load(file)

        return details

    def update_file(self, filename) -> None:
        query = self.get_filename_query(filename)

        db.execute_and_save_query(query, filename)

        with open(self.json_file_path, "r") as file:
            details = json.load(file)

        for file_detail in details:
            if filename in file_detail["fileName"]:
                file_detail["updatedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                break

        # save file back into json file
        with open(self.json_file_path, "w") as file:
            json.dump(details, file, indent=4)


ApiLogicInstance = ApiLogic()
