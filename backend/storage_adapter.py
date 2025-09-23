# storage_adapter.py (cleaned)
import os
import json
import logging
from pathlib import Path
from datetime import timedelta
from typing import Optional
import base64
import tempfile
import threading
import mimetypes

from google.oauth2 import service_account
from google.cloud import storage as gcs_storage

logger = logging.getLogger("filetagapi.storage_adapter")
logger.addHandler(logging.NullHandler())

# Small util to read boolean env flags
def _env_true_any(*names, default="false"):
    for n in names:
        v = os.getenv(n)
        if v is not None:
            return str(v).strip().lower() in ("1", "true", "yes", "on")
    return str(default).strip().lower() in ("1", "true", "yes", "on")

# Configuration
USE_GCS = _env_true_any("USE_GCS", "GCS_ENABLED", default="false")
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip() or None

# Lazy holders with lock
_init_lock = threading.Lock()
_gcs_client: Optional[gcs_storage.Client] = None
_gcs_bucket: Optional[gcs_storage.Bucket] = None


def _ensure_gcs_ready():
    global _gcs_client, _gcs_bucket
    if not USE_GCS:
        raise RuntimeError("GCS usage not enabled (USE_GCS is false)")
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET env var not set but USE_GCS is true")
    # fast path
    if _gcs_client is not None and _gcs_bucket is not None:
        return _gcs_client, _gcs_bucket

    with _init_lock:
        if _gcs_client is not None and _gcs_bucket is not None:
            return _gcs_client, _gcs_bucket
        try:
            client = gcs_storage.Client()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize GCS client: {e}") from e

        bucket = client.bucket(GCS_BUCKET)
        try:
            if not bucket.exists():
                raise RuntimeError(f"GCS bucket '{GCS_BUCKET}' does not exist or is not accessible by the configured credentials.")
        except Exception as e:
            raise RuntimeError(f"Failed to access GCS bucket '{GCS_BUCKET}': {e}") from e

        _gcs_client = client
        _gcs_bucket = bucket
        logger.info("GCS ready - bucket=%s", GCS_BUCKET)
        return _gcs_client, _gcs_bucket

def save_file_bytes(company: str, survey: str, filename: str, bytes_data: bytes) -> str:
    """
    GCS-only: upload bytes to gs://{GCS_BUCKET}/{company}/{survey}/{filename}
    Raises RuntimeError if USE_GCS is not enabled or bucket missing.
    Returns the gs:// URL on success.
    """
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if not USE_GCS:
        raise RuntimeError("GCS is not enabled (USE_GCS=false). This adapter is configured for GCS-only operation.")

    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET is not configured")

    try:
        _, bucket = _ensure_gcs_ready()
        blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
        blob = bucket.blob(blob_path)
        content_type = mimetypes.guess_type(filename_safe)[0] or "application/octet-stream"
        logger.info("Uploading to GCS: gs://%s/%s (bytes=%d) content_type=%s", bucket.name, blob_path, len(bytes_data), content_type)
        blob.upload_from_string(bytes_data, content_type=content_type)
        return f"gs://{bucket.name}/{blob_path}"
    except Exception:
        logger.exception("GCS upload failed for %s/%s/%s", company_safe, survey_safe, filename_safe)
        raise


def save_file_from_path(company: str, survey: str, filename: str, local_path: Path) -> str:
    """
    Upload a local file to GCS and return gs:// path.
    Raises RuntimeError if USE_GCS is false or GCS not configured.
    """
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if not USE_GCS:
        raise RuntimeError("GCS is not enabled (USE_GCS=false). This adapter is configured for GCS-only operation.")

    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET is not configured")

    try:
        _, bucket = _ensure_gcs_ready()
        blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
        blob = bucket.blob(blob_path)
        logger.info("Uploading file to GCS from path: %s -> gs://%s/%s", local_path, bucket.name, blob_path)
        blob.upload_from_filename(str(local_path))
        return f"gs://{bucket.name}/{blob_path}"
    except Exception:
        logger.exception("GCS file upload from path failed for %s", local_path)
        raise


def _parse_service_account_env(sa_env: str):
    """
    Parse GCP_SA_KEY which can be:
      - path to file
      - raw JSON
      - base64-encoded JSON
    Returns dict.
    """
    if os.path.exists(sa_env):
        with open(sa_env, "r", encoding="utf-8") as f:
            return json.load(f)
    # try raw JSON
    try:
        return json.loads(sa_env)
    except Exception:
        # try base64
        try:
            decoded = base64.b64decode(sa_env).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError("GCP_SA_KEY is not a valid JSON string, file path, or base64-encoded JSON") from e

def get_signed_url(company: str, survey: str, filename: str, expires_seconds: int = 3600,
                   response_disposition: Optional[str] = None, response_type: Optional[str] = None) -> str:
    company_safe = str(company).strip()
    survey_safe = str(survey).strip()
    filename_safe = str(filename).strip()

    if not USE_GCS:
        raise RuntimeError("GCS is not enabled (USE_GCS=false). Cannot generate signed URL in GCS-only mode.")


    blob_path = f"{company_safe}/{survey_safe}/{filename_safe}"
    try:
        _, bucket = _ensure_gcs_ready()
        blob = bucket.blob(blob_path)
        logger.info("Generating signed URL for gs://%s/%s (expires=%ds)", bucket.name, blob_path, expires_seconds)

        sign_kwargs = {"expiration": timedelta(seconds=expires_seconds), "version": "v4"}
        if response_disposition:
            sign_kwargs["response_disposition"] = response_disposition
        if response_type:
            sign_kwargs["response_type"] = response_type

        try:
            url = blob.generate_signed_url(**sign_kwargs)
            logger.info("Signed URL generated using default credentials")
            return url
        except Exception as e_default:
            logger.warning("Default generate_signed_url failed (%s). Will attempt fallback using GCP_SA_KEY if available.", e_default)

        sa_env = os.getenv("GCP_SA_KEY")
        if not sa_env:
            raise RuntimeError("Default credentials cannot sign URLs and GCP_SA_KEY env var not set; cannot generate signed URL.")

        sa_info = _parse_service_account_env(sa_env)
        sa_creds = service_account.Credentials.from_service_account_info(sa_info)
        try:
            # pass explicit credentials to signer
            url = blob.generate_signed_url(credentials=sa_creds, **sign_kwargs)
            logger.info("Signed URL generated using service-account JSON (GCP_SA_KEY)")
            return url
        except Exception as e:
            logger.exception("Fallback generate_signed_url with service account JSON failed")
            raise RuntimeError(f"Fallback generate_signed_url failed: {e}") from e

    except Exception as final_err:
        logger.exception("Failed to generate signed URL for %s", blob_path)
        raise
