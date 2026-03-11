from pathlib import Path

import pandas as pd
import requests


def fetch_coinbase_candles(product_id: str = "BTC-USD", granularity: int = 3600) -> pd.DataFrame:
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    params = {
        "granularity": granularity
    }

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


def save_to_csv(df, product_id="BTC-USD", interval="1h"):

    # location of this script
    script_dir = Path(__file__).resolve().parent

    # project root (one folder above collectors)
    project_root = script_dir.parent

    # data directory
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)

    file_path = data_dir / f"{product_id}_{interval}.csv"

    df.to_csv(file_path, index=False)

    print("Saved file to:", file_path)

    return file_path


def main() -> None:
    print("Downloading BTC market data from Coinbase...")

    df = fetch_coinbase_candles(product_id="BTC-USD", granularity=3600)

    print("\nPreview:")
    print(df.head())

    file_path = save_to_csv(df, product_id="BTC-USD", interval="1h")

    print(f"\nSaved to: {file_path}")
    print(f"Rows saved: {len(df)}")


if __name__ == "__main__":
    main()