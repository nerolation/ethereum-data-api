import os
import io
import pandas as pd
from cryptography.fernet import Fernet
from fastapi import FastAPI, Depends, HTTPException, status, Request,Security
from fastapi.security import  HTTPBasic, HTTPBasicCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import sqlite3
from sqlalchemy import create_engine
from base64 import b64decode

testing = True

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

# Fetch the key from environment variables
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


def authenticate_user(username: str, password: str):
    if username in df.keys():
        if df[username] == password:
            return True
    return False  

database_url = os.environ.get('database_url')
def get_beaconchain_data_by_slot(day: int):
    engine = create_engine(database_url)
    df = pd.read_sql_query(f"SELECT * FROM slots_of_day_{day}", engine)
    return df.to_json(orient="records")

@app.get("/beaconchain/{day}")
@limiter.limit("5/minute")
async def get_beaconchain_slot(day: int, request: Request, credentials: HTTPBasicCredentials = Security(security)):
    username = credentials.username
    password = credentials.password

    if not authenticate_user(username, password):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

    day_data = get_beaconchain_data_by_slot(day)
    if len(day_data) < 1:
        raise HTTPException(status_code=404, detail="No data found for the given slot")
    return {"data": day_data}


# Handling rate limit errors
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    return PlainTextResponse(str(exc), status_code=429)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
