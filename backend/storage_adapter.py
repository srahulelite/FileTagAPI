# storage_adapter.py
# Minimal storage adapter supporting local filesystem (default) and Google Cloud Storage (if USE_GCS=true).
# - save_file_bytes(company, survey, filename, bytes_data) -> returns stored path (local path or gs://...).
# - get_signed_url(company, survey, filename, expires_seconds=3600) -> returns URL to download (signed URL for GCS, local API download path otherwise).

import os
from pathlib import Path
from datetime import timedelta

USE_GCS = os.getenv("USE_GCS", "false").lower() == "true"
GCS_BUCKET = os.getenv("GCS_BUCKET", "")

if USE_GCS:
    try:
        from google.cloud import storage
    except Exception as e:
        raise RuntimeError("google-cloud-storage not installed or failed to import. Install with `pip install google-cloud-storage`.") from e
    _gcs_client = storage.Client()
    _gcs_bucket = _gcs_client.bucket(GCS_BUCKET)

def _local_base():
    base = Path(os.getenv("BASE_UPLOAD_DIR", "uploads"))
    base.mkdir(parents=True, exist_ok=True)
    return base

def save_file_bytes(company: str, survey: str, filename: str, bytes_data: bytes):
    """
    Save raw bytes. Returns a string path:
      - Local mode: 'uploads/{company}/{survey}/{filename}'
      - GCS mode: 'gs://{bucket}/{company}/{survey}/{filename}'
    """
    company_safe = str(company)
    survey_safe = str(survey)
    if USE_GCS:
        blob_path = f"{company_safe}/{survey_safe}/{filename}"
        blob = _gcs_bucket.blob(blob_path)
        blob.upload_from_string(bytes_data)
        return f"gs://{_gcs_bucket.name}/{blob_path}"
    else:
        base = _local_base() / company_safe / survey_safe
        base.mkdir(parents=True, exist_ok=True)
        p = base / filename
        p.write_bytes(bytes_data)
        return str(p)

def save_file_from_path(company: str, survey: str, filename: str, local_path: Path):
    """Upload local file to GCS if USE_GCS, otherwise return local path; returns same style string as save_file_bytes."""
    if USE_GCS:
        blob_path = f"{company}/{survey}/{filename}"
        blob = _gcs_bucket.blob(blob_path)
        blob.upload_from_filename(str(local_path))
        return f"gs://{_gcs_bucket.name}/{blob_path}"
    else:
        # file already at local_path expected to be under uploads; return local path str
        return str(local_path)

def get_signed_url(company: str, survey: str, filename: str, expires_seconds: int = 3600):
    """
    Returns a URL accessible by the client:
      - GCS: signed URL (expires)
      - Local: path to our download endpoint (client must hit /api/v1/{company}/surveys/{survey}/download/{filename})
    """
    company_safe = str(company)
    survey_safe = str(survey)
    if USE_GCS:
        blob_path = f"{company_safe}/{survey_safe}/{filename}"
        blob = _gcs_bucket.blob(blob_path)
        url = blob.generate_signed_url(expiration=timedelta(seconds=expires_seconds))
        return url
    else:
        # local URL (same-origin). Consumer will call this to download; ensure your app serves download route.
        return f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{filename}"
