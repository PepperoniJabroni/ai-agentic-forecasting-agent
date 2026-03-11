from pathlib import Path
import pandas as pd
import requests


def fetch_coinbase_candles(product_id: str = "BTC-USD", granularity: int = 3600) -> pd.DataFrame:
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    params = {"granularity": granularity}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(
        data,
        columns=["timestamp", "low", "high", "open", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    for col in ["low", "high", "open", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])

    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def get_data_file_path(product_id: str = "BTC-USD", interval: str = "1h") -> Path:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)

    return data_dir / f"{product_id}_{interval}.csv"


def load_existing_data(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        return pd.DataFrame(columns=["timestamp", "low", "high", "open", "close", "volume"])

    df = pd.read_csv(file_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def update_dataset(product_id: str = "BTC-USD", interval: str = "1h", granularity: int = 3600) -> Path:
    file_path = get_data_file_path(product_id=product_id, interval=interval)

    print(f"Loading existing data from: {file_path}")
    existing_df = load_existing_data(file_path)
    print(f"Existing rows: {len(existing_df)}")

    print(f"Fetching latest candles for {product_id}...")
    new_df = fetch_coinbase_candles(product_id=product_id, granularity=granularity)
    print(f"Fetched rows: {len(new_df)}")

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    before_dedup = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=["timestamp"], keep="last")
    after_dedup = len(combined_df)

    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)

    combined_df.to_csv(file_path, index=False)

    print(f"Rows before dedup: {before_dedup}")
    print(f"Rows after dedup: {after_dedup}")
    print(f"Newest candle: {combined_df['timestamp'].max()}")
    print(f"Saved updated dataset to: {file_path}")

    return file_path


def main() -> None:
    file_path = update_dataset(product_id="BTC-USD", interval="1h", granularity=3600)

    df = pd.read_csv(file_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    print("\nOldest candle:")
    print(df["timestamp"].min())

    print("\nNewest candle:")
    print(df["timestamp"].max())

    print("\nLast 5 rows:")
    print(df.tail())


if __name__ == "__main__":
    main()