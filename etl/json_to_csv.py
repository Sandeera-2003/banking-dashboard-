import csv
import json
from pathlib import Path

data_folder = Path(__file__).resolve().parents[1] / "dataSources"
source = data_folder / "finance_external_feed.json"
destination = data_folder / "finance_external_feed.csv"

columns = {
    "customer_id": "customer_id",
    "emp.var.rate": "emp_var_rate",
    "cons.price.idx": "cons_price_idx",
    "cons.conf.idx": "cons_conf_idx",
    "euribor3m": "euribor3m",
    "nr.employed": "nr_employed",
}

with source.open("r", encoding="utf-8") as file:
    records = json.load(file)

with destination.open("w", newline="", encoding="utf-8-sig") as file:
    writer = csv.DictWriter(file, fieldnames=columns.values())
    writer.writeheader()
    for record in records:
        writer.writerow({new: record[old] for old, new in columns.items()})

print(f"Created {destination.name} with {len(records)} rows")