import os
import io
import pandas as pd
from cryptography.fernet import Fernet
from fastapi import FastAPI, Depends, HTTPException, status, Request, Security, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from base64 import b64decode



testing = False

security = HTTPBasic()


# Initialize FastAPI and Limiter
app = FastAPI()
limiter = Limiter(key_func=get_remote_address)

if testing:
    try:
        with open("./config/decryption_key", "r") as file:
            os.environ["decryption_key"] = file.read().strip()
        with open("./config/database_url", "r") as file:
            os.environ["database_url"] = file.read().strip()
    except:
        raise ValueError("Problem with ./config/ settings")

database_url = os.environ.get("database_url")
key = os.environ.get("decryption_key")
fernet = Fernet(key)

# Read encrypted data
with open('users.parquet.encrypted', 'rb') as file:
    encrypted_data = file.read()

# Decrypt data
decrypted_data = fernet.decrypt(encrypted_data)

# Load decrypted data into DataFrame
df = pd.read_parquet(io.BytesIO(decrypted_data))
df = df.set_index("username").to_dict()["password"]
print(df)


async def authenticate_user(username: str, password: str):
    print(username)
    print(df.keys())
    if username in df.keys():
        if df[username] == password:
            return True
    return False


engine = create_async_engine(database_url, echo=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_beaconchain_data_by_slot(day: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(f"SELECT * FROM slots_of_day_{day}"))
        df = pd.DataFrame(result.fetchall())
        return df.to_json(orient="records")


@app.get("/beaconchain/{day}")
@limiter.limit("5/minute")
async def get_beaconchain_slot(day: int, request: Request, credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password

    if not await authenticate_user(username, password):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

    day_data = await get_beaconchain_data_by_slot(day)
    if not day_data:
        raise HTTPException(status_code=404, detail="No data found for the given slot")
    return {"data": day_data}

# Handling rate limit errors
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    return PlainTextResponse(str(exc), status_code=429)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
