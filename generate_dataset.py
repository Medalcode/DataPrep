"""
generate_dataset.py
-------------------
Generates a synthetic 'dirty' sales dataset for testing the DataPrep Pipeline.
Introduces intentional data quality issues: nulls, duplicates, bad formats, etc.
"""
import random
import string
from pathlib import Path
import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

OUTPUT_PATH = Path(__file__).parent / "data" / "raw" / "ventas.csv"


def random_date(start="2022-01-01", end="2024-12-31") -> str:
    start_dt = pd.Timestamp(start)
    end_dt   = pd.Timestamp(end)
    delta    = (end_dt - start_dt).days
    rand_day = random.randint(0, delta)
    return str(start_dt + pd.Timedelta(days=rand_day))


PRODUCTS   = ["Laptop", "Teclado", "Monitor", "Mouse", "Auriculares", "Webcam", "Hub USB", "SSD"]
CATEGORIES = ["Electrónica", "Periféricos", "Almacenamiento", "Audio"]
REGIONS    = ["Norte", "Sur", "Centro", "Oriente", "Occidente"]
SELLERS    = ["Juan Pérez", "María García", "Carlos López", "Ana Torres", "Pedro Soto"]


def make_clean_row(i: int) -> dict:
    product  = random.choice(PRODUCTS)
    category = random.choice(CATEGORIES)
    price    = round(random.uniform(15, 2000), 2)
    qty      = random.randint(1, 50)
    return {
        "id_venta":    i,
        "fecha":       random_date(),
        "producto":    product,
        "categoria":   category,
        "precio":      price,
        "cantidad":    qty,
        "region":      random.choice(REGIONS),
        "vendedor":    random.choice(SELLERS),
        "descuento":   round(random.uniform(0, 0.3), 2),
    }


def introduce_errors(df: pd.DataFrame, null_pct: float = 0.12, dup_pct: float = 0.05) -> pd.DataFrame:
    """Introduce realistic data quality issues."""
    df = df.copy()
    n_rows  = len(df)
    n_nulls = int(n_rows * null_pct)
    n_dups  = int(n_rows * dup_pct)

    # 1️⃣ Nulls — random cells in several columns
    for col in ["precio", "cantidad", "region", "vendedor", "fecha", "descuento"]:
        null_indices = np.random.choice(df.index, size=int(n_nulls / 6), replace=False)
        df.loc[null_indices, col] = np.nan

    # 2️⃣ Bad date formats
    bad_date_idx = np.random.choice(df.index, size=30, replace=False)
    for idx in bad_date_idx:
        day   = random.randint(1, 28)
        month = random.randint(1, 12)
        year  = random.randint(2022, 2024)
        df.at[idx, "fecha"] = f"{day}/{month}/{year}"  # Wrong format

    # 3️⃣ Extra whitespace in strings
    ws_idx = np.random.choice(df.index, size=60, replace=False)
    for idx in ws_idx:
        df.at[idx, "producto"] = "  " + str(df.at[idx, "producto"]) + "  "
        df.at[idx, "region"]   = str(df.at[idx, "region"]) + " "

    # 4️⃣ Inconsistent casing
    case_idx = np.random.choice(df.index, size=40, replace=False)
    for idx in case_idx:
        val = df.at[idx, "categoria"]
        if isinstance(val, str):
            df.at[idx, "categoria"] = val.upper()

    # 5️⃣ Negative prices (corrupted)
    neg_idx = np.random.choice(df.index, size=15, replace=False)
    df.loc[neg_idx, "precio"] = df.loc[neg_idx, "precio"].abs() * -1

    # 6️⃣ Duplicate rows
    dup_source = df.sample(n=n_dups, random_state=99)
    df = pd.concat([df, dup_source], ignore_index=True)

    # 7️⃣ Mixed type column — sell_code column with numbers + strings mixed
    codes = []
    for i in range(len(df)):
        if random.random() < 0.15:
            codes.append("N/A")
        elif random.random() < 0.05:
            codes.append(f"ERR-{''.join(random.choices(string.ascii_uppercase, k=3))}")
        else:
            codes.append(str(random.randint(1000, 9999)))
    df["codigo_venta"] = codes

    return df.sample(frac=1, random_state=7).reset_index(drop=True)


def generate_dataset(n: int = 1000) -> pd.DataFrame:
    records = [make_clean_row(i + 1) for i in range(n)]
    df = pd.DataFrame(records)
    df = introduce_errors(df)
    return df


if __name__ == "__main__":
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = generate_dataset(1000)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"✅ Dataset generado: {OUTPUT_PATH}")
    print(f"   Filas totales  : {len(df):,}")
    print(f"   Columnas       : {list(df.columns)}")
    print(f"\nVista previa de problemas:")
    print(f"   Nulos totales  : {df.isnull().sum().sum()}")
    print(f"   Duplicados     : {df.duplicated().sum()}")
    print(f"\n{df.head(5).to_string()}")
