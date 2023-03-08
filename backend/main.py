from typing import Union
from fastapi import FastAPI 
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import pandas as pd
import re


app = FastAPI()

app.add_middleware(
    TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1"] 
)


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get( "/symbols" )
def read_symbols() -> list:
    df = pd.read_csv("../dags/sample_index.csv", header=0)
    symbols = df["Name"].values.tolist()
    symbol_options = [ {"label": symbol, "value": symbol} for symbol in symbols]
    return symbol_options


@app.get("/symbol/{symbol}")
def read_data( symbol: str ):
    symbol = re.sub(r"_", "/", symbol)
    df = pd.read_csv("../dags/db.csv", header=0)
    df_symbol = df[ df["symbol"] == symbol ]
    return df_symbol.to_dict(orient="list")

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    df = pd.read_csv("../dags/db.csv", header=0)
    print( df )
    return {"item_id": item_id, "q": q}