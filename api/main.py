from fastapi import FastAPI, HTTPException, Depends, status
from services.routes import router as api_router
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from models import User, UserCreate, UserLogin, UserResponse, TokenRefreshRequest
from jose import JWTError, jwt
import uvicorn
import logging
from database import SessionLocal, engine
from utils.auth_utils import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
    verify_token,
    SECRET_KEY,
    ALGORITHM,
)

app = FastAPI()

origin = ["http://localhost:8080", "http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origin,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


logging.basicConfig(level=logging.INFO)
app.include_router(api_router, prefix="/api", tags=["api"])


##include business router
app.include_router(api_router)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Protected route that requires a valid access token
@app.get("/protected")
def read_protected_data(current_user: str = Depends(verify_token)):
    return {"message": f"Hello, {current_user}! You are authenticated."}


@app.get("/")
def testing():
    return {"message": "Hello, World!"}


# register route
@app.post("/users/", response_model=UserResponse)
def create_user_route(user: UserCreate, db: Session = Depends(get_db)):

    check_user = db.query(User).filter(User.username == user.username).first()

    if check_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists"
        )

    hashed_password = hash_password(user.password)
    db_user = User(username=user.username, hashed_password=hashed_password)

    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return JSONResponse(
        content={"message": "User has been created", "username": db_user.username},
        status_code=status.HTTP_201_CREATED,
    )


# login (get tokens)
@app.post("/login")
def login_for_access_token(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()
    if not db_user or not verify_password(user.password, db_user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    # Generate access and refresh tokens
    access_token = create_access_token(data={"sub": db_user.username})
    refresh_token = create_refresh_token(data={"sub": db_user.username})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@app.post("/token/refresh")
def refresh_access_token(token_refresh_request: TokenRefreshRequest):
    refresh_token = token_refresh_request.refresh_token
    try:
        # Decode the refresh token
        payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")

        # If username is missing, raise an exception
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        # Create a new access token using the username from the refresh token
        access_token = create_access_token(data={"sub": username})
        return {"access_token": access_token, "token_type": "bearer"}

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")


# Run the app
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
