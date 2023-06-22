import requests
import json
import sqlite3
from sqlite3 import Error
from datetime import datetime, timedelta
import twitter_config as config
import tweepy
import time
from telegram import Bot
import asyncio

telegram_token = 'YOURTOKEN'
telegram_chat_id = 'CHATID'

bot = Bot(token=telegram_token)

async def send_telegram_message(message):
    await bot.send_message(chat_id=telegram_chat_id, text=message)

# Add your Twitter API keys here
consumer_key = config.CONSUMER_KEY
consumer_secret = config.CONSUMER_SECRET
access_token = config.ACCESS_TOKEN
access_token_secret = config.ACCESS_TOKEN_SECRET

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

url = "https://api.metalblockchain.org/ext/P"

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,nl;q=0.8,und;q=0.7",
    "content-type": "application/json",
    "sec-ch-ua": "\"Google Chrome\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site"
}

data = {
    "jsonrpc":"2.0",
    "method":"platform.getCurrentValidators",
    "params":{
        "subnetID":"11111111111111111111111111111111LpoYY"
    },
    "id":1
}

def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('metalnodes.db')
        return conn
    except Error as e:
        print(e)

def add_last_seen_column_if_not_exists(conn):
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(validators)")
    columns = cur.fetchall()
    column_names = [column[1] for column in columns]

    if 'last_seen' not in column_names:
        cur.execute("ALTER TABLE validators ADD COLUMN last_seen REAL")
        conn.commit()

def create_table(conn):
    try:
        sql_create_validators_table = """CREATE TABLE IF NOT EXISTS validators (
                                            date text NOT NULL,
                                            node_id text NOT NULL,
                                            uptime real,
                                            fee real,
                                            end_time real,
                                            PRIMARY KEY (date, node_id)
                                        );"""
        cur = conn.cursor()
        cur.execute(sql_create_validators_table)
        add_last_seen_column_if_not_exists(conn)
    except Error as e:
        print(e)

def upsert_validator_record(conn, validator):
    sql = '''INSERT INTO validators(date, node_id, uptime, fee, end_time, last_seen)
              VALUES(?,?,?,?,?,?)
              ON CONFLICT(date, node_id) DO UPDATE SET
              uptime = excluded.uptime,
              fee = excluded.fee,
              end_time = excluded.end_time,
              last_seen = excluded.last_seen;'''
    cur = conn.cursor()
    #print(f"Upserting validator: {validator}")  # Print the validator data
    cur.execute(sql, validator)
    conn.commit()
    #print("Upserted validator")  # Print a message after the record is upserted

def get_validator_record(conn, node_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM validators WHERE node_id=? ORDER BY date DESC LIMIT 1", (node_id,))
    row = cur.fetchone()
    return row

async def store_validator_data(validators):
    conn = create_connection()
    if conn is not None:
        create_table(conn)
        for validator in validators:
            date = datetime.now().strftime('%Y-%m-%d')
            node_id = validator.get('nodeID')
            uptime = validator.get('uptime')
            fee = validator.get('delegationFee')
            end_time = int(validator.get('endTime'))  # Convert end_time to integer
            last_seen = datetime.now().timestamp()

            # Check if validator already exists in the database
            existing_validator = get_validator_record(conn, node_id)

            if not existing_validator or (existing_validator and existing_validator[-1] is not None and (last_seen - existing_validator[-1]) > 172800):
                # Calculate end_time in days from now
                end_time_days = (datetime.fromtimestamp(end_time) - datetime.now()).days

                # Tweet about the new validator
                tweet_text = (f"New #MetalBlockchain Validator Node Found\n"
                              f"{node_id}\n"
                              f"Fee: {fee}%\n"
                              f"End Time: {end_time_days} Days\n"
                              f"\n"
                              f"Follow @MetalNodes for more! $METAL")

                try:
                    # Send the tweet
                    api.update_status(tweet_text)
                    print("Tweeted about new validator node")
                except Exception as e:
                    print(f"Error occurred while sending tweet: {e}")

                # Send the telegram message
                try:
                    await send_telegram_message(tweet_text)
                except Exception as e:
                    print(f"Error occurred while sending telegram message: {e}")

            # Always upsert the record, regardless of whether the validator already exists in the database
            upsert_validator_record(conn, (date, node_id, uptime, fee, end_time, last_seen))
    else:
        print("Error! Cannot create the database connection.")

async def main_loop():
    while True:
        try:
            print("Fetching data from the Metal Blockchain API...")  # Added print statement
            response = requests.post(url, headers=headers, data=json.dumps(data))
            json_response = response.json()
            validators = json_response.get('result', {}).get('validators', [])
            print(f"Found {len(validators)} validators")  # Added print statement
            await store_validator_data(validators)
            print("Stored validator data")  # Added print statement
        except (requests.exceptions.RequestException, ValueError):
            print("Error occurred while fetching data from the Metal Blockchain API.")
            await asyncio.sleep(60)  # Delay for 60 seconds before retrying
            continue

        print("Checked for new Metal Nodes. Sleeping for 30 minutes...")
        await asyncio.sleep(1800)  # Delay for 1800 seconds (30 minutes)


if __name__ == "__main__":
    asyncio.run(main_loop())
