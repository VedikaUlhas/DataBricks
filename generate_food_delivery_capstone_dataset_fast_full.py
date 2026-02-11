#!/usr/bin/env python3
# Food Delivery Capstone v2 — FAST Dataset Generator (from scratch)
# Vectorized heavy tables • Shuffled clean+bad • Indian Faker (en_IN) • --quick smoke mode
# Usage:
#   python generate_food_delivery_capstone_dataset_fast_full.py --out ./food_delivery_capstone_v2 --seed 23
#   python generate_food_delivery_capstone_dataset_fast_full.py --out ./food_delivery_capstone_v2_quick --seed 23 --quick

from __future__ import annotations
import argparse, json, random, math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List

import numpy as np
import pandas as pd
from faker import Faker


# ---------------------------- CLI & Utilities ----------------------------

def build_arg_parser():
    p = argparse.ArgumentParser("FAST food-delivery dataset generator")
    p.add_argument("--out", required=True, help="Output folder (will create data/merged and logs)")
    p.add_argument("--seed", type=int, default=23, help="Random seed for determinism")
    p.add_argument("--no-progress", action="store_true", help="Silence progress logs")
    p.add_argument("--parquet", action="store_true", help="Also write Parquet files")
    p.add_argument("--quick", action="store_true", help="Generate ~10% rows (smoke test)")
    return p

class Progress:
    def __init__(self, quiet: bool): self.quiet = quiet
    def log(self, msg: str):
        if not self.quiet: print(msg, flush=True)

def ensure_dirs(root: Path):
    data_dir = root / "data" / "merged"
    logs_dir = root / "logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, logs_dir

def build_faker(seed:int) -> Faker:
    fake = Faker("en_IN")
    Faker.seed(seed); random.seed(seed)
    return fake

def shuffle_and_write(df: pd.DataFrame, out_csv: Path, out_parquet: bool, seed: int):
    df = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    df.to_csv(out_csv, index=False)
    if out_parquet:
        try:
            df.to_parquet(out_csv.with_suffix(".parquet"), index=False)
        except Exception as e:
            # Parquet optional; keep CSVs
            pass
    return len(df)


# ---------------------------- Generators (Dims) ----------------------------

def gen_customers(clean_n:int, bad_n:int, rng, fake:Faker) -> Tuple[pd.DataFrame, pd.DataFrame]:
    genders = ["male","female"]
    rows: List[dict] = []
    for i in range(1, clean_n+1):
        g = rng.choice(genders)
        first = fake.first_name_male() if g=="male" else fake.first_name_female()
        last = fake.last_name()
        rows.append({
            "customer_id": f"CU{i:07d}",
            "first_name": first,
            "last_name": last,
            "gender": g,
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "created_at": (datetime.now() - timedelta(days=int(rng.integers(60, 800)))).isoformat(sep=" ")
        })
    clean_df = pd.DataFrame(rows)

    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["missing","dup_key","type_mismatch","future_date"])
        g = rng.choice(genders)
        first = fake.first_name_male() if g=="male" else fake.first_name_female()
        last = fake.last_name()
        rec = {
            "customer_id": f"CU{base:07d}",
            "first_name": first,
            "last_name": last,
            "gender": g,
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "created_at": (datetime.now() - timedelta(days=int(rng.integers(1, 800)))).isoformat(sep=" ")
        }
        if kind=="missing":
            rec[rng.choice(["email","city","first_name"])] = None
        elif kind=="dup_key":
            rec["customer_id"] = f"CU{int(rng.integers(1, clean_n+1)):07d}"
        elif kind=="type_mismatch":
            rec["gender"] = 1
        elif kind=="future_date":
            rec["created_at"] = (datetime.now() + timedelta(days=int(rng.integers(1, 120)))).isoformat(sep=" ")
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df

def gen_restaurants(clean_n:int, bad_n:int, rng, fake:Faker) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    cuisines = ["Indian","Chinese","Italian","Thai","Mexican","American","Bakery","Cafe","Japanese"]
    for i in range(1, clean_n+1):
        rows.append({
            "restaurant_id": f"RS{i:06d}",
            "restaurant_name": f"{fake.last_name()} {rng.choice(['Foods','Kitchen','Bites','Diner','Cafe'])}",
            "cuisine": rng.choice(cuisines),
            "city": fake.city(),
            "state": fake.state(),
            "rating": round(float(rng.uniform(2.0, 5.0)), 1),
            "is_active": True
        })
    clean_df = pd.DataFrame(rows)

    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["missing","dup_key","type_mismatch","negative"])
        rec = {
            "restaurant_id": f"RS{base:06d}",
            "restaurant_name": f"{fake.color_name()} {rng.choice(['Eatery','Dhaba','Tadka'])}",
            "cuisine": rng.choice(cuisines+[None]),
            "city": fake.city(),
            "state": fake.state(),
            "rating": round(float(rng.uniform(2.0, 5.0)), 1),
            "is_active": True
        }
        if kind=="missing":
            rec["restaurant_name"] = None
        elif kind=="dup_key":
            rec["restaurant_id"] = f"RS{int(rng.integers(1, clean_n+1)):06d}"
        elif kind=="type_mismatch":
            rec["rating"] = "FIVE"
        elif kind=="negative":
            rec["rating"] = -1.0
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df

def gen_couriers(clean_n:int, bad_n:int, rng, fake:Faker) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    modes = ["bike","scooter","cycle"]
    for i in range(1, clean_n+1):
        rows.append({
            "courier_id": f"CR{i:06d}",
            "courier_name": f"{fake.first_name()} {fake.last_name()}",
            "vehicle_type": rng.choice(modes),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state(),
            "is_active": True
        })
    clean_df = pd.DataFrame(rows)

    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["missing","dup_key","type_mismatch"])
        rec = {
            "courier_id": f"CR{base:06d}",
            "courier_name": f"{fake.first_name()} {fake.last_name()}",
            "vehicle_type": rng.choice(modes+[None]),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state(),
            "is_active": True
        }
        if kind=="missing": rec["courier_name"] = None
        elif kind=="dup_key": rec["courier_id"] = f"CR{int(rng.integers(1, clean_n+1)):06d}"
        elif kind=="type_mismatch": rec["is_active"] = "yes"
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df


# ---------------------------- Generators (Facts) ----------------------------

def gen_orders(clean_n:int, bad_n:int, rng, fake:Faker, customer_max:int, restaurant_max:int, dup_id_max:int|None=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    statuses = ["PLACED","PREPARING","EN_ROUTE","DELIVERED","CANCELLED"]
    for i in range(1, clean_n+1):
        cid = int(rng.integers(1, customer_max+1))
        rid = int(rng.integers(1, restaurant_max+1))
        dt = datetime.now() - timedelta(days=int(rng.integers(1, 400)))
        rows.append({
            "order_id": f"OD{i:08d}",
            "customer_id": f"CU{cid:07d}",
            "restaurant_id": f"RS{rid:06d}",
            "order_datetime": dt.isoformat(sep=" "),
            "status": rng.choice(statuses),
            "city": fake.city(),
            "state": fake.state()
        })
    clean_df = pd.DataFrame(rows)

    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["orphan_customer","orphan_restaurant","future_date","dup_key","missing"])
        cid = int(rng.integers(1, customer_max+1))
        rid = int(rng.integers(1, restaurant_max+1))
        dt = datetime.now() - timedelta(days=int(rng.integers(1, 400)))
        rec = {
            "order_id": f"OD{base:08d}",
            "customer_id": f"CU{cid:07d}",
            "restaurant_id": f"RS{rid:06d}",
            "order_datetime": dt.isoformat(sep=" "),
            "status": rng.choice(statuses),
            "city": fake.city(),
            "state": fake.state()
        }
        if kind=="orphan_customer": rec["customer_id"] = "__ORPHAN__"
        elif kind=="orphan_restaurant": rec["restaurant_id"] = "__ORPHAN__"
        elif kind=="future_date": rec["order_datetime"] = (datetime.now()+timedelta(days=int(rng.integers(1,120)))).isoformat(sep=" ")
        elif kind=="dup_key":
            high = (dup_id_max if dup_id_max and dup_id_max>0 else max(clean_n,1))
            dup_idx = 1 if high==1 else int(rng.integers(1, high+1))
            rec["order_id"] = f"OD{dup_idx:08d}"
        elif kind=="missing":
            rec["state"] = None
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df

def gen_order_items_vectorized(clean_n:int, bad_n:int, rng, order_clean_max:int, menu_catalog_size:int, dup_id_max:int|None=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Clean (vectorized)
    oid = rng.integers(1, order_clean_max + 1, size=clean_n, dtype=np.int64)
    qty = rng.integers(1, 5, size=clean_n, dtype=np.int64)
    price = rng.uniform(50.0, 1200.0, size=clean_n)
    code_idx = rng.integers(1, menu_catalog_size + 1, size=clean_n, dtype=np.int64)
    clean_df = pd.DataFrame({
        "order_item_id": [f"OI{i:09d}" for i in range(1, clean_n+1)],
        "order_id": [f"OD{x:08d}" for x in oid],
        "menu_code": [f"MN{m:05d}" for m in code_idx],
        "quantity": qty,
        "unit_price": np.round(price, 2),
        "line_amount": np.round(qty * price, 2)
    })
    # Bad
    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["orphan_order","negative","type_mismatch","dup_key","orphan_menu"])
        oid_ = int(rng.integers(1, order_clean_max+1))
        code = f"MN{int(rng.integers(1, menu_catalog_size+1)):05d}"
        q = int(rng.integers(1,5))
        price_ = float(round(rng.uniform(50, 1200), 2))
        rec = {
            "order_item_id": f"OI{base:09d}",
            "order_id": f"OD{oid_:08d}",
            "menu_code": code,
            "quantity": q,
            "unit_price": price_,
            "line_amount": round(q*price_, 2)
        }
        if kind=="orphan_order": rec["order_id"] = "__ORPHAN__"
        elif kind=="negative": rec["quantity"] = -abs(int(rng.integers(1,4)))
        elif kind=="type_mismatch": rec["unit_price"] = "PRICE"
        elif kind=="dup_key":
            high = (dup_id_max if dup_id_max and dup_id_max>0 else max(clean_n,1))
            dup_idx = 1 if high==1 else int(rng.integers(1, high+1))
            rec["order_item_id"] = f"OI{dup_idx:09d}"
        elif kind=="orphan_menu": rec["menu_code"] = "__ORPHAN__"
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df

def gen_deliveries_vectorized(clean_n:int, bad_n:int, rng, order_clean_max:int, courier_clean_max:int, dup_id_max:int|None=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Clean (vectorized datetimes)
    oid = rng.integers(1, order_clean_max + 1, size=clean_n, dtype=np.int64)
    cid = rng.integers(1, courier_clean_max + 1, size=clean_n, dtype=np.int64)
    assign_days = rng.integers(1, 400, size=clean_n, dtype=np.int64)
    assign = np.array([datetime.now() - timedelta(days=int(x)) for x in assign_days])
    mins = rng.integers(10, 120, size=clean_n, dtype=np.int64)
    drop = assign + np.array([timedelta(minutes=int(m)) for m in mins])
    dist = np.round(rng.uniform(0.5, 20.0, size=clean_n), 2)
    clean_df = pd.DataFrame({
        "delivery_id": [f"DV{i:08d}" for i in range(1, clean_n+1)],
        "order_id": [f"OD{x:08d}" for x in oid],
        "courier_id": [f"CR{x:06d}" for x in cid],
        "assign_time": assign.astype('datetime64[s]').astype(str),
        "drop_time": drop.astype('datetime64[s]').astype(str),
        "distance_km": dist
    })
    # Bad
    rows = []
    for j in range(bad_n):
        base = clean_n + j + 1
        kind = rng.choice(["orphan_order","orphan_courier","negative","type_mismatch","dup_key"])
        oid_ = int(rng.integers(1, order_clean_max+1))
        cid_ = int(rng.integers(1, courier_clean_max+1))
        assign_ = (datetime.now() - timedelta(days=int(rng.integers(1, 400))))
        drop_ = assign_ + timedelta(minutes=int(rng.integers(10, 120)))
        rec = {
            "delivery_id": f"DV{base:08d}",
            "order_id": f"OD{oid_:08d}",
            "courier_id": f"CR{cid_:06d}",
            "assign_time": assign_.isoformat(sep=" "),
            "drop_time": drop_.isoformat(sep=" "),
            "distance_km": float(round(rng.uniform(0.5, 20.0), 2))
        }
        if kind=="orphan_order": rec["order_id"] = "__ORPHAN__"
        elif kind=="orphan_courier": rec["courier_id"] = "__ORPHAN__"
        elif kind=="negative": rec["distance_km"] = -abs(float(rng.uniform(0.5, 10.0)))
        elif kind=="type_mismatch": rec["distance_km"] = "FAR"
        elif kind=="dup_key":
            high = (dup_id_max if dup_id_max and dup_id_max>0 else max(clean_n,1))
            dup_idx = 1 if high==1 else int(rng.integers(1, high+1))
            rec["delivery_id"] = f"DV{dup_idx:08d}"
        rows.append(rec)
    bad_df = pd.DataFrame(rows)
    return clean_df, bad_df


# ---------------------------- Main Orchestration ----------------------------

def main():
    args = build_arg_parser().parse_args()
    root = Path(args.out).resolve()
    data_dir, logs_dir = ensure_dirs(root)
    progress = Progress(args.no_progress)
    seed = int(args.seed)
    rng = np.random.default_rng(seed)
    fake = build_faker(seed)

    # Target sizes (≈1.4M merged)
    COUNTS = {
        "customers": (210_000, 30_000),
        "restaurants": (70_000, 10_000),
        "couriers": (65_000, 10_000),
        "orders": (170_000, 30_000),
        "order_items": (550_000, 90_000),
        "deliveries": (135_000, 30_000),
    }
    if args.quick:
        scale = 0.10  # ~10%
        COUNTS = {k: (max(1000, math.ceil(v[0]*scale)), max(200, math.ceil(v[1]*scale))) for k, v in COUNTS.items()}

    progress.log("Generating customers ...")
    customers_clean, customers_bad = gen_customers(*COUNTS["customers"], rng, fake)

    progress.log("Generating restaurants ...")
    restaurants_clean, restaurants_bad = gen_restaurants(*COUNTS["restaurants"], rng, fake)

    progress.log("Generating couriers ...")
    couriers_clean, couriers_bad = gen_couriers(*COUNTS["couriers"], rng, fake)

    progress.log("Generating orders ...")
    orders_clean, orders_bad = gen_orders(COUNTS["orders"][0], COUNTS["orders"][1], rng, fake,
                                          customer_max=COUNTS["customers"][0],
                                          restaurant_max=COUNTS["restaurants"][0],
                                          dup_id_max=COUNTS["orders"][0])

    progress.log("Generating order_items (vectorized) ...")
    oi_clean, oi_bad = gen_order_items_vectorized(COUNTS["order_items"][0], COUNTS["order_items"][1], rng,
                                                  order_clean_max=COUNTS["orders"][0],
                                                  menu_catalog_size=20000,
                                                  dup_id_max=COUNTS["order_items"][0])

    progress.log("Generating deliveries (vectorized) ...")
    deliv_clean, deliv_bad = gen_deliveries_vectorized(COUNTS["deliveries"][0], COUNTS["deliveries"][1], rng,
                                                       order_clean_max=COUNTS["orders"][0],
                                                       courier_clean_max=COUNTS["couriers"][0],
                                                       dup_id_max=COUNTS["deliveries"][0])

    # Merge + shuffle + write
    progress.log("Writing merged files ...")
    totals = {}
    totals["customers"] = shuffle_and_write(pd.concat([customers_clean, customers_bad], ignore_index=True),
                                            data_dir / "customers_merged.csv", args.parquet, seed)
    totals["restaurants"] = shuffle_and_write(pd.concat([restaurants_clean, restaurants_bad], ignore_index=True),
                                              data_dir / "restaurants_merged.csv", args.parquet, seed)
    totals["couriers"] = shuffle_and_write(pd.concat([couriers_clean, couriers_bad], ignore_index=True),
                                           data_dir / "couriers_merged.csv", args.parquet, seed)
    totals["orders"] = shuffle_and_write(pd.concat([orders_clean, orders_bad], ignore_index=True),
                                         data_dir / "orders_merged.csv", args.parquet, seed)
    totals["order_items"] = shuffle_and_write(pd.concat([oi_clean, oi_bad], ignore_index=True),
                                             data_dir / "order_items_merged.csv", args.parquet, seed)
    totals["deliveries"] = shuffle_and_write(pd.concat([deliv_clean, deliv_bad], ignore_index=True),
                                            data_dir / "deliveries_merged.csv", args.parquet, seed)

    profiling = {
        "seed": seed,
        "quick": bool(args.quick),
        "orphan_placeholder": "__ORPHAN__",
        "entities": {k: int(v) for k, v in totals.items()},
        "grand_totals": {"merged_sum": int(sum(totals.values()))},
        "notes": [
            "Vectorized order_items & deliveries for speed",
            "Indian-localized Faker used on dimensions",
            "Concatenate clean+bad then shuffle before write"
        ]
    }
    (logs_dir / "profiling_summary.json").write_text(json.dumps(profiling, indent=2))

    (logs_dir / "generation_config.json").write_text(json.dumps({
        "version": "food-delivery-v2.2-fast-from-scratch",
        "faker_locale": "en_IN",
        "shuffle": True,
        "quick": bool(args.quick),
        "write_parquet": bool(args.parquet),
        "targets": COUNTS,
        "output_dir": str(root)
    }, indent=2))

    progress.log("Done. Files written to: " + str(root))


if __name__ == "__main__":
    main()
