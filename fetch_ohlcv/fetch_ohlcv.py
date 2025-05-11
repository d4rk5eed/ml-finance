import argparse
import ccxt
import pandas as pd
import time
from datetime import datetime
import sys

def fetch_ohlcv_data(symbol, timeframe, limit_records, batch_size=300):
    """
    Выгрузка исторических данных с пагинацией
    """
    exchange = ccxt.coinbase({
        'enableRateLimit': True,
        'rateLimit': 300,
    })

    timeframe_seconds = exchange.parse_timeframe(timeframe) * 1000
    since = exchange.milliseconds() - (limit_records * timeframe_seconds)

    all_data = []
    remaining = limit_records

    while remaining > 0:
        try:
            current_limit = min(batch_size, remaining)
            print(f"Fetching since {datetime.utcfromtimestamp(since / 1000.0)}", file=sys.stderr)
            data = exchange.fetch_ohlcv(
                symbol,
                timeframe,
                since=since,
                limit=current_limit
            )

            if not data:
                break

            all_data.extend(data)
            remaining -= len(data)
            since = data[-1][0] + timeframe_seconds

            time.sleep(exchange.rateLimit / 1000)

        except Exception as e:
            print(f"Error: {e}, retrying...", file=sys.stderr)
            time.sleep(5)

    return all_data[:limit_records]

def main():
    parser = argparse.ArgumentParser(description='Fetch OHLCV data from Coinbase')
    parser.add_argument('-N', type=int, default=10000, help='Number of records to fetch')
    parser.add_argument('-T', type=str, default='BTC/USDT', help='Ticker symbol')
    parser.add_argument('-I', type=str, default='5m', help='Time interval')
    parser.add_argument('-o', type=str, help='Output CSV file')

    args = parser.parse_args()

    print(f"Fetching {args.N} records for {args.T} ({args.I})...", file=sys.stderr)
    data = fetch_ohlcv_data(args.T, args.I, args.N)

    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

    if args.o:
        df.to_csv(args.o, index=False)
        print(f"Data saved to {args.o}")
    else:
        print(df.to_csv(index=False))

if __name__ == "__main__":
    main()
