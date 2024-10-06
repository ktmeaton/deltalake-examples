#!/usr/bin/env python

from deltalake import DeltaTable,write_deltalake
from deltalake.table import TableOptimizer
import pandas as pd
import os

table_path = "data/python"
log_retention = "interval 60 seconds"

# Add a first batch of records
df = pd.DataFrame({ "sample": ["A", "B"], "year": [ 2023, 2024]})
print("\n" + "-" * 80)
print(f"First record batch:\n{df.to_markdown(index=False, tablefmt='outline')}")
write_deltalake(
    table_or_uri=table_path, 
    data=df, 
    mode="overwrite",
    partition_by=["year"],
    configuration={"delta.logRetentionDuration": log_retention}
)

# Merge in a second batch of records
df = pd.DataFrame({ "sample": ["A", "B", "C"], "year": [ 2022, 2024, 2024] })
print("\n" + "-" * 80)
print(f"Second record batch:\n{df.to_markdown(index=False, tablefmt='outline')}")
(
    DeltaTable(table_path)
    .merge(
        source=df,
        predicate="source.sample = target.sample",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)

dt = DeltaTable(table_path)
print("\n" + "-" * 80)
print(f"Final Delta Table:\n{dt.to_pandas().to_markdown(index=False, tablefmt='outline')}")

# If you don't want to keep old files from previous versions, use 'vacuum' to clean them up!
print("\n" + "-" * 80)
print(f"Cleaning up unused data files:")
deleted_files = DeltaTable(table_path).vacuum(retention_hours=0, enforce_retention_duration=False)
for file in deleted_files:
    file_path = os.path.join(table_path, file)
    print(f"\t{file_path}")
    os.remove(file_path)

# If you don't want to keep old files from previous versions, use 'vacuum' to clean them up!
print("\n" + "-" * 80)
print(f"Cleaning up logs older than: {log_retention}.")
DeltaTable(table_path).create_checkpoint()
DeltaTable(table_path).cleanup_metadata()

print("\n" + "-" * 80)
print(f"File Structure ({table_path}):\n")
for root, dirs, files in os.walk(table_path):
    level = root.replace(table_path, '').count(os.sep)
    indent = ' ' * 4 * (level)
    print('{}{}/'.format(indent, os.path.basename(root)))
    subindent = ' ' * 4 * (level + 1)
    for f in files:
        print('{}{}'.format(subindent, f))

print("\n" + "-" * 80)
print(f"History:\n")
dt = DeltaTable(table_path)
for transaction in dt.history():
    print(f"\t{transaction}"[0:100] + "...")