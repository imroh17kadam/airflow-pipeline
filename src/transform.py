# src/transform.py
import logging

def transform_data(raw_data: list) -> list:
    logging.info("Starting data transformation")

    transformed = []

    for record in raw_data:
        if "id" not in record or "title" not in record:
            continue

        transformed.append({
            "id": record["id"],
            "title": record["title"].strip(),
            "completed": record.get("completed", False)
        })

    if not transformed:
        raise Exception("No valid records after transformation")

    logging.info(f"Transformed {len(transformed)} records")
    return transformed