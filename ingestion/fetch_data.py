import requests
import json
import time
from datetime import datetime
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = "bitcoin,ethereum,cardano,solana,polkadot"
VS_CURRENCIES = "usd"
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("DB_NAME", "market_data")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

def get_db_connection():
    print(f"Connecting to {DB_HOST} as {DB_USER} with password: {DB_PASSWORD}")
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def fetch_crypto_prices():
    """
    Fetches current prices for specified cryptocurrencies from CoinGecko.
    """
    try:
        params = {
            "ids": COINS,
            "vs_currencies": VS_CURRENCIES,
            "include_last_updated_at": "true"
        }
        
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        timestamp = datetime.now()
        print(f"[{timestamp}] Successfully fetched data")
        
        save_to_db(data)
        
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def save_to_db(data):
    """
    Saves fetched data to PostgreSQL database.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        for symbol, info in data.items():
            price = info['usd']
            # last_updated_at from API is a timestamp, but we can also use current time
            
            cur.execute(
                "INSERT INTO crypto_prices (symbol, price_usd, fetched_at) VALUES (%s, %s, %s)",
                (symbol, price, datetime.now())
            )
        
        conn.commit()
        cur.close()
        print("Data saved to database successfully")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error saving to database: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    fetch_crypto_prices()
