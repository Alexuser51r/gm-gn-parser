import os
import json
from datetime import datetime
from typing import List, Tuple

CACHE_DIR = "/home/chromeuser/screens/token_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _contract_to_filename(contract_address: str) -> str:
    return os.path.join(CACHE_DIR, f"{contract_address}.json")

def load_token_history(contract_address: str) -> List[Tuple[str, float]]:
    path = _contract_to_filename(contract_address)
    if not os.path.exists(path):
        return []

    with open(path, "r") as f:
        raw_data = json.load(f)

    return [(datetime.strptime(k, "%Y-%m-%d %H:%M"), v) for k, v in raw_data.items()]

def save_token_history(contract_address: str, timeline: List[Tuple[datetime, str]]):
    path = _contract_to_filename(contract_address)

    existing = {}
    if os.path.exists(path):
        with open(path, "r") as f:
            existing = json.load(f)

    for entry in timeline:
        if not isinstance(entry, tuple) or len(entry) != 2:
            print(f"[CACHE][SKIP] ❌ Невалидный элемент (не кортеж): {entry}")
            continue

        dt, val = entry
        if not isinstance(dt, datetime) or not isinstance(val, str):
            print(f"[CACHE][SKIP] ❌ Пропуск — dt: {dt}, val: {val}")
            continue

        key = dt.strftime("%Y-%m-%d %H:%M")
        if key not in existing:
            existing[key] = val

    with open(path, "w") as f:
        json.dump(existing, f, indent=2)
        print(f"[CACHE] ✅ История сохранена для {contract_address} ({len(existing)} записей)")
