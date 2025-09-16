# # storage_adapter.py
# # Minimal storage adapter supporting local filesystem (default) and Google Cloud Storage (if USE_GCS=true).
# # - save_file_bytes(company, survey, filename, bytes_data) -> returns stored path (local path or gs://...).
# # - get_signed_url(company, survey, filename, expires_seconds=3600) -> returns URL to download (signed URL for GCS, local API download path otherwise).

# import os
# from pathlib import Path
# from datetime import timedelta

# USE_GCS = os.getenv("USE_GCS", "false").lower() == "true"
# GCS_BUCKET = os.getenv("GCS_BUCKET", "")

# if USE_GCS:
#     try:
#         from google.cloud import storage
#     except Exception as e:
#         raise RuntimeError("google-cloud-storage not installed or failed to import. Install with `pip install google-cloud-storage`.") from e
#     _gcs_client = storage.Client()
#     _gcs_bucket = _gcs_client.bucket(GCS_BUCKET)

# def _local_base():
#     base = Path(os.getenv("BASE_UPLOAD_DIR", "uploads"))
#     base.mkdir(parents=True, exist_ok=True)
#     return base

# def save_file_bytes(company: str, survey: str, filename: str, bytes_data: bytes):
#     """
#     Save raw bytes. Returns a string path:
#       - Local mode: 'uploads/{company}/{survey}/{filename}'
#       - GCS mode: 'gs://{bucket}/{company}/{survey}/{filename}'
#     """
#     company_safe = str(company)
#     survey_safe = str(survey)
#     if USE_GCS:
#         blob_path = f"{company_safe}/{survey_safe}/{filename}"
#         blob = _gcs_bucket.blob(blob_path)
#         blob.upload_from_string(bytes_data)
#         return f"gs://{_gcs_bucket.name}/{blob_path}"
#     else:
#         base = _local_base() / company_safe / survey_safe
#         base.mkdir(parents=True, exist_ok=True)
#         p = base / filename
#         p.write_bytes(bytes_data)
#         return str(p)

# def save_file_from_path(company: str, survey: str, filename: str, local_path: Path):
#     """Upload local file to GCS if USE_GCS, otherwise return local path; returns same style string as save_file_bytes."""
#     if USE_GCS:
#         blob_path = f"{company}/{survey}/{filename}"
#         blob = _gcs_bucket.blob(blob_path)
#         blob.upload_from_filename(str(local_path))
#         return f"gs://{_gcs_bucket.name}/{blob_path}"
#     else:
#         # file already at local_path expected to be under uploads; return local path str
#         return str(local_path)

# def get_signed_url(company: str, survey: str, filename: str, expires_seconds: int = 3600):
#     """
#     Returns a URL accessible by the client:
#       - GCS: signed URL (expires)
#       - Local: path to our download endpoint (client must hit /api/v1/{company}/surveys/{survey}/download/{filename})
#     """
#     company_safe = str(company)
#     survey_safe = str(survey)
#     if USE_GCS:
#         blob_path = f"{company_safe}/{survey_safe}/{filename}"
#         blob = _gcs_bucket.blob(blob_path)
#         url = blob.generate_signed_url(expiration=timedelta(seconds=expires_seconds))
#         return url
#     else:
#         # local URL (same-origin). Consumer will call this to download; ensure your app serves download route.
#         return f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{filename}"

# storage_adapter.py (robust replacement)
import os
import logging
from pathlib import Path
from datetime import timedelta
from typing import Optional

import os
import json
import logging
from datetime import timedelta
from google.oauth2 import service_account
logger = logging.getLogger("storage_adapter")

import os

def _env_true_any(*names, default="false"):
    for n in names:
        v = os.getenv(n)
        if v is not None:
            return str(v).strip().lower() in ("1","true","yes","on")
    return str(default).strip().lower() in ("1","true","yes","on")

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Accept either USE_GCS or the older GCS_ENABLED flag
USE_GCS = _env_true_any("USE_GCS", "GCS_ENABLED", default="false")
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()

# Lazy holders
_gcs_client = None
_gcs_bucket = None

def _local_base() -> Path:
    base = Path(os.getenv("BASE_UPLOAD_DIR", "uploads"))
    base.mkdir(parents=True, exist_ok=True)
    return base

def _ensure_gcs_ready():
    """
    Initialize _gcs_client and _gcs_bucket lazily and validate configuration.
    Raises RuntimeError on bad config/credentials.
    """
    global _gcs_client, _gcs_bucket
    if not USE_GCS:
        raise RuntimeError("GCS usage not enabled (USE_GCS is false)")

    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET env var not set but USE_GCS is true")

    if _gcs_client is not None and _gcs_bucket is not None:
        return _gcs_client, _gcs_bucket

    try:
        from google.cloud import storage
    except Exception as e:
        raise RuntimeError("google-cloud-storage not installed or failed to import. Install with `pip install google-cloud-storage`.") from e

    try:
        _gcs_client = storage.Client()
    except Exception as e:
        raise RuntimeError(f"Failed to initialize GCS client: {e}") from e

    try:
        _gcs_bucket = _gcs_client.bucket(GCS_BUCKET)
        # validate bucket exists / accessible (this does a small check)
        if not _gcs_bucket.exists():
            raise RuntimeError(f"GCS bucket '{GCS_BUCKET}' does not exist or is not accessible by the configured credentials.")
    except Exception as e:
        # reset to avoid partially-initialized state
        _gcs_client = None
        _gcs_bucket = None
        raise RuntimeError(f"Failed to access GCS bucket '{GCS_BUCKET}': {e}") from e

    logger.info("GCS ready - bucket=%s", GCS_BUCKET)
    return _gcs_client, _gcs_bucket

def save_file_bytes(company: str, survey: str, filename: str, bytes_data: bytes) -> str:
    """
    Saves raw bytes.
    Returns:
      - Local mode: path string under uploads/... (absolute or relative depending on BASE_UPLOAD_DIR)
      - GCS mode: 'gs://{bucket}/{company}/{survey}/{filename}'
    """
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if USE_GCS:
        try:
            _, bucket = _ensure_gcs_ready()
            blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
            blob = bucket.blob(blob_path)
            logger.info("Uploading to GCS: gs://%s/%s (bytes=%d)", bucket.name, blob_path, len(bytes_data))
            blob.upload_from_string(bytes_data)
            return f"gs://{bucket.name}/{blob_path}"
        except Exception as e:
            logger.exception("GCS upload failed for %s/%s/%s", company_safe, survey_safe, filename_safe)
            raise

    # Local fallback
    base = _local_base() / company_safe / survey_safe
    base.mkdir(parents=True, exist_ok=True)
    p = base / filename_safe
    logger.info("Saving local file: %s", p)
    p.write_bytes(bytes_data)
    return str(p)

def save_file_from_path(company: str, survey: str, filename: str, local_path: Path) -> str:
    """
    Upload local file to GCS if USE_GCS, otherwise return local_path as str.
    """
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if USE_GCS:
        try:
            _, bucket = _ensure_gcs_ready()
            blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
            blob = bucket.blob(blob_path)
            logger.info("Uploading file to GCS from path: %s -> gs://%s/%s", local_path, bucket.name, blob_path)
            blob.upload_from_filename(str(local_path))
            return f"gs://{bucket.name}/{blob_path}"
        except Exception as e:
            logger.exception("GCS file upload from path failed")
            raise
    else:
        # local path assumed already in uploads; ensure it's readable
        logger.info("Using local path (no GCS): %s", local_path)
        return str(local_path)

def get_signed_url(company: str, survey: str, filename: str, expires_seconds: int = 3600):
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if not USE_GCS:
        return f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{filename_safe}"

    blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
    try:
        _, bucket = _ensure_gcs_ready()
        blob = bucket.blob(blob_path)
        logger.info("Generating signed URL for gs://%s/%s (expires=%ds)", bucket.name, blob_path, expires_seconds)

        # 1) Try default signing (works if credentials contain private key)
        try:
            url = blob.generate_signed_url(expiration=timedelta(seconds=expires_seconds))
            logger.info("Signed URL generated using default credentials")
            return url
        except Exception as first_err:
            # Only log and fall back; keep the exception for debugging
            logger.warning("Default generate_signed_url failed (%s). Will attempt fallback using service-account JSON from GCP_SA_KEY.", first_err)

        # 2) Fallback: attempt to read service-account JSON provided via Secret Manager through env var
        sa_env = os.getenv("GCP_SA_KEY")
        if not sa_env:
            raise RuntimeError("Default credentials cannot sign URLs and GCP_SA_KEY env var not set; cannot generate signed URL.")

        # Try to interpret sa_env as either a path to JSON file or raw JSON string
        sa_info = None
        try:
            if os.path.exists(sa_env):
                logger.info("GCP_SA_KEY points to a file path; loading JSON from file")
                with open(sa_env, "r", encoding="utf-8") as f:
                    sa_info = json.load(f)
            else:
                # treat as raw JSON string
                # log only the length, not the contents, for safety
                logger.info("GCP_SA_KEY provided as env string of length %d", len(sa_env))
                sa_info = json.loads(sa_env)
        except Exception as e:
            raise RuntimeError(f"Failed to parse service account JSON from GCP_SA_KEY: {e}") from e

        # Build credentials from the service account info and use it to sign
        try:
            sa_creds = service_account.Credentials.from_service_account_info(sa_info)
        except Exception as e:
            raise RuntimeError(f"Failed to build credentials from service account info: {e}") from e

        # Now generate signed URL using explicit credentials (this uses the private key)
        try:
            url = blob.generate_signed_url(expiration=timedelta(seconds=expires_seconds), credentials=sa_creds)
            logger.info("Signed URL generated using fallback service account JSON (via GCP_SA_KEY)")
            return url
        except Exception as e:
            logger.exception("Fallback generate_signed_url with service account JSON failed")
            raise

    except Exception:
        logger.exception("Failed to generate signed URL for %s", blob_path)
        # raise upward so caller returns 500 (or handle gracefully if you prefer)
        raise
