from fastapi import APIRouter
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from services.logic import ApiLogicInstance
from utils.auth_utils import verify_token
import os
import logging
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)


class FileUpdateRequest(BaseModel):
    filename: str


router = APIRouter()


@router.get("/files")
async def get_files(current_user: str = Depends(verify_token)):
    details = ApiLogicInstance.get_filenames_details()
    return details


@router.post("/download/")
async def download_file(request: Request, current_user: str = Depends(verify_token)):
    data = await request.json()
    filename = data.get("filename")

    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    file_path = os.path.join("../api/data", filename + ".csv")
    logging.info(f"Requested file path: {file_path}")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found {file_path}")

    return FileResponse(file_path, status_code=200, filename=filename)


@router.put("/files/")
async def update_file(
    request: FileUpdateRequest,
):
    filename = request.filename

    logging.info(f"Updating file {filename}")
    try:
        ApiLogicInstance.update_file(filename)
        return {"message": f"File {filename} updated successfully"}
    except Exception as e:
        logging.error(f"Error updating file: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating file: {e}")
