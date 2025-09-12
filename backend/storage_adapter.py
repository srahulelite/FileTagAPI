# storage_adapter.py
import os
from pathlib import Path
from datetime import timedelta

USE_GCS = os.getenv("USE_GCS", "false").lower() == "true"
if USE_GCS:
    from google.cloud import storage
    client = storage.Client()
    BUCKET = client.bucket(os.getenv("GCS_BUCKET"))

def save_file_bytes(company, survey, filename, data_bytes):
    if USE_GCS:
        blob_path = f"{company}/{survey}/{filename}"
        blob = BUCKET.blob(blob_path)
        blob.upload_from_string(data_bytes)
        return f"gs://{BUCKET.name}/{blob_path}"
    else:
        base = Path("uploads") / company / survey
        base.mkdir(parents=True, exist_ok=True)
        p = base / filename
        p.write_bytes(data_bytes)
        return str(p)

def get_signed_url(company, survey, filename, expires_seconds=3600):
    if USE_GCS:
        blob_path = f"{company}/{survey}/{filename}"
        blob = BUCKET.blob(blob_path)
        return blob.generate_signed_url(expiration=timedelta(seconds=expires_seconds))
    else:
        return f"/api/v1/{company}/surveys/{survey}/download/{filename}"
