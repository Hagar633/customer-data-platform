from pathlib import Path
import pandas as pd


# Project root:
# customer-data-platform/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def inspect_file(file_path: Path):
    df = pd.read_parquet(file_path)

    print("=" * 70)
    print(f"FILE: {file_path.name}")
    print("=" * 70)

    print(f"\nRows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumns:")
    for column in df.columns:
        print(f"  - {column}")

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    missing = df.isnull().sum()

    if missing.sum() == 0:
        print("  No missing values")
    else:
        print(missing[missing > 0])

    print(f"\nDuplicate rows: {df.duplicated().sum():,}")

    print("\nFirst 5 rows:")
    print(df.head())

    print()


def main():
    print(f"Looking for data in: {RAW_DATA_DIR}\n")

    files = sorted(RAW_DATA_DIR.glob("*.parquet"))

    if not files:
        print("No Parquet files found.")
        return

    print(f"Found {len(files)} Parquet files.\n")

    for file_path in files:
        inspect_file(file_path)


if __name__ == "__main__":
    main()