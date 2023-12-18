import os
import io
import uvicorn
import pandas as pd
from cryptography.fernet import Fernet
from fastapi import FastAPI, Depends, HTTPException, Request, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

testing = False

security = HTTPBasic()

limiter = Limiter(key_func=get_remote_address)

# Initialize FastAPI and Limiter
app = FastAPI(title="DotPics Data API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)



if testing:
    try:
        with open("./config/decryption_key", "r") as file:
            os.environ["decryption_key"] = file.read().strip()
        with open("./config/database_url", "r") as file:
            os.environ["HEROKU_POSTGRESQL_BLUE_URL"] = file.read().strip()
    except:
        raise ValueError("Problem with ./config/ settings")


database_url = os.environ.get("HEROKU_POSTGRESQL_BLUE_URL").replace("postgres://", "postgresql+asyncpg://")
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


async def authenticate_user(username: str, password: str):
    if username in df.keys():
        if df[username] == password:
            return True
    return False

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    return PlainTextResponse(f"Rate limit exceeded. Please slow down to 1 request per second", status_code=429)

engine = create_async_engine(database_url, echo=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_beaconchain_data_by_slot(day: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(f"SELECT * FROM slots_of_day_{day}"))
        df = pd.DataFrame(result.fetchall())
        return df.to_json(orient="records")

    
async def get_validator_by_index(index: int):
    dataset_name = "beaconchain_validators_db"
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(f"SELECT validator_id, pubkey, withdrawn, label, label2 FROM {dataset_name} WHERE validator_id = :index"), {"index": index})
        row = result.fetchone()
        if row:
            return {
                "validator_id": row[0],
                "pubkey": row[1],
                "withdrawn": row[2],
                "label": row[3],
                "label2": row[4]
            }
        return None


@app.get("/beaconchain/{day}")
@limiter.limit("60/hour")
async def get_beaconchain_slot(request: Request, day: int, credentials: HTTPBasicCredentials = Depends(security)):
    if not await authenticate_user(credentials.username, credentials.password):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

    day_data = await get_beaconchain_data_by_slot(day)
    if not day_data:
        raise HTTPException(status_code=404, detail="No data found for the given slot")
    return {"data": day_data}


@app.get("/validator/{index}")
@limiter.limit("1/hour")
async def get_validators(request: Request, index: int, credentials: HTTPBasicCredentials = Depends(security)):
    if not await authenticate_user(credentials.username, credentials.password):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

    validator_data = await get_validator_by_index(index)
    if not validator_data:
        raise HTTPException(status_code=404, detail="No data found for the given validator")
    return {"data": validator_data}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
