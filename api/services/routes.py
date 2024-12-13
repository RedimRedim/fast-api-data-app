from fastapi import APIRouter
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse
from services.logic import ApiLogicInstance
from sqlalchemy.orm import Session
from utils.auth_utils import verify_token
import os
import logging
from pydantic import BaseModel
from database import get_db
from models import User

# Configure logging
logging.basicConfig(level=logging.INFO)


class FileUpdateRequest(BaseModel):
    db: str
    filename: str


router = APIRouter()


@router.get("/mysql/files")
async def get_files(
    request: FileUpdateRequest,
):
    try:
        filename = request.filename
        await ApiLogicInstance.update_mysql(filename=filename)
        return {"message": f" {filename} finished successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving file: {e}")


@router.get("/files")
async def get_files(
    current_user: str = Depends(verify_token), db: Session = Depends(get_db)
):

    check_user = db.query(User).filter(User.username == current_user).first()

    if not check_user:
        raise HTTPException(status_code=401, detail="User not found")

    details = ApiLogicInstance.get_filenames_details(role=check_user.role)
    return details


@router.post("/download/")
async def download_file(request: Request, current_user: str = Depends(verify_token)):
    data = await request.json()
    filename = data.get("filename").lower()

    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    file_path = os.path.join("../api/data", filename + ".csv")
    logging.info(f"Requested file path: {file_path}")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found {file_path}")

    return FileResponse(file_path, status_code=200, filename=filename)


@router.put("/files")
async def update_file(
    request: FileUpdateRequest,
):
    db = request.db
    filename = request.filename.lower()

    logging.info(f"Updating file {filename}")
    try:
        await ApiLogicInstance.update_file(db, filename)
        return {"message": f"File {filename} updated successfully"}
    except Exception as e:
        logging.error(f"Error updating file: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating file: {e}")
