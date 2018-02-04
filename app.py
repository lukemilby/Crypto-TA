import json # Read settings.json
import talib # Run Technical Analysis on dataframe
import ccxt # Pull Data to TA
import pandas as pd # Build and manipulate dataframe
from time import sleep
import sqlite3

# Create Database
conn = sqlite3.connect(':memory:')
c = conn.cursor()


# Assigning settings
settings = json.load(open('settings.json'))
market_pair = settings['market_pair']
timeframe = settings['time_frame']
exchange = getattr(ccxt,settings['exchange'])()
lastdateindf = 0
timeframeValues = {'5m':300,'15m':900,'30m':1800,'1h':3600,'2h':7200,'4h':14400,'6h':21600,'12h':43200,'1d':86400}
df = []

def create_dbtable(c, conn):
    # Create Table ohlcv
    c.execute('''
        CREATE TABLE ohlcv(datetime INTEGER, open INTEGER, high INTEGER, low INTEGER, close INTEGER, volume INTEGER)
        ''')
    conn.commit()
    c.execute('CREATE TABLE rsi(rsi_value')


def import_data(data, c, conn):
    # Take fetched data and import to db
    for items in data:
        for item in items:
            c.execute('INSERT INTO ohlcv values(?,?,?,?,?,?)', (item))
            conn.commit()


def generate_rsi(c, conn):
    df = pd.read_sql_query("SELECT close FROM ohlcv", conn)
    close = df.close.values
    rsi = talib.RSI(close, 14)
    return rsi

def update_db(data, c, conn):
    for item in data:
        c.execute('INSERT INTO ohlcv values(?,?,?,?,?,?)', (item))
        conn.commit()


def get_last_time(c, conn):
    # Gets last timeframe added
    c.execute("SELECT datetime FROM ohlcv ORDER BY datetime DESC LIMIT 1")
    result = c.fetchone()
    return result[0]



# Pull data for market pair, exhange and timeframe
def init_fetchData(market_pair, timeframe):
    return exchange.fetch_ohlcv(market_pair,timeframe=timeframe)


def fetch_current_data(lastdateindf, market_pair, timeframe,timeframeValues):
    return exchange.fetch_ohlcv(market_pair, timeframe=timeframe, since=(lastdateindf +(timeframeValues[timeframe]*1000)))


def generate_rsi(close):
    return talib.RSI(close, timeperiod=14)
###########################################
#                MAIN                     #
###########################################

def main():
    # Build exchange object
    # Fetch data
    data = init_fetchData(market_pair, timeframe),
    # Build database
    create_dbtable(c , conn)
    print("DB table set")
    import_data(data, c, conn)
    print("Initial data imported")

    # Assign last datetime added to the DB
    lastdateindf = get_last_time( c, conn)

    while True:
        try:
            # Try and fetch new data for the current datetime
            new_data = fetch_current_data(lastdateindf, market_pair, timeframe, timeframeValues)
            print("Next Bar at > {}".format(lastdateindf + (timeframeValues[timeframe]*1000)))
        except e:
            print('Timeout occurred')
        print(new_data)
        if new_data:
            # if we have successfully retrieved the new data add it to the DB and update the last datetime pointer
            update_db(new_data, c, conn)
            print('Imported new data >', new_data)
            lastdateindf = get_last_time(c , conn)
            print('Updated last datetime')
        sleep(20)

        

if __name__ == '__main__':
    main()