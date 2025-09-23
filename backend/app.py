# app.py
# ------------ CLEANED TOP-OF-FILE SNIPPET (copy-paste) ------------
from pathlib import Path
from typing import Optional, List
import os
import re
import io
import mimetypes
import subprocess
import tempfile
import secrets
import logging
from PIL import Image

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Request, Header, Depends, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from datetime import datetime, timezone, timedelta

# project modules
from auth import get_key_record, create_api_key, increment_usage_and_check, init_db as auth_init_db
from auth import insert_log as log_event, add_random_tags_for_file, get_tags
from storage_adapter import save_file_bytes, save_file_from_path, get_signed_url, USE_GCS as ADAPTER_USE_GCS

# google client (used only when needed)
from google.cloud import storage
import google.cloud.storage as gcs_storage  # used in optimize helper

# Config
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB max
ALLOWED_PREFIXES = ("image/", "video/")
VIDEO_EXTS = {'.mp4', '.webm', '.ogg', '.mov', '.m4v', '.avi', '.flv', '.mkv'}

# Storage config: keep boolean toggle separate from bucket name
GCS_ENABLED: bool = bool(ADAPTER_USE_GCS)
GCS_BUCKET: Optional[str] = os.getenv("GCS_BUCKET") or getattr(__import__("storage_adapter"), "GCS_BUCKET", None)


# Logging
logger = logging.getLogger("filetagapi")
logging.basicConfig(level=logging.INFO)
logger.info("STARTUP: GCS_ENABLED=%s GCS_BUCKET=%s GOOGLE_APPLICATION_CREDENTIALS=%s CWD=%s",
            GCS_ENABLED, GCS_BUCKET, os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), os.getcwd())

# FastAPI app + templates + static
app = FastAPI(title="Upload Service")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# init DBs for auth / logs (these should be CloudSQL-backed in production)
auth_init_db()

# CORS dev helper
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Simple filename sanitizer to avoid path traversal and odd chars
_filename_re = re.compile(r"[^a-zA-Z0-9\-\._]")

def secure_name(name: str) -> str:
    if not name:
        return "file"
    # remove directories, keep only basename
    name = Path(name).name
    # replace disallowed chars
    name = _filename_re.sub("_", name)
    return name

def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path

def verify_api_key(company: str, x_api_key: str = Header(...)):
    """
    Dependency: path param 'company' will be passed by FastAPI automatically.
    Raises HTTPException if invalid or over quota.
    """
    rec = get_key_record(x_api_key)
    if not rec:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    rec_company, rec_key, daily_limit = rec[0], rec[1], rec[2]
    # ensure company matches key
    if rec_company != company:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="API key not valid for company")
    ok, count, limit = increment_usage_and_check(x_api_key)
    if not ok:
        raise HTTPException(status_code=429, detail=f"Daily quota exceeded ({count}/{limit})")
    # optionally return usage info
    return {"company": company, "api_key": x_api_key, "usage": {"today": count, "limit": limit}}


def generate_api_key(nbytes: int = 24) -> str:
    # produces URL-safe token, e.g. ~32 chars
    return secrets.token_urlsafe(nbytes)

# http://127.0.0.1:8000/register
@app.get("/register", response_class=HTMLResponse)
async def register_get(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

# http://127.0.0.1:8000/register
@app.post("/register", response_class=HTMLResponse)
async def register_post(request: Request, company: str = Form(...)):
    company_safe = secure_name(company).lower()
    # Prefer using auth helpers if present
    # try to use get_key_record_for_company() if auth exposes it
    api_key = None
    try:
        # if auth module exposes helper, use it:
        from auth import get_key_record_for_company, create_api_key  # optional
        rec = None
        try:
            rec = get_key_record_for_company(company_safe)
        except Exception:
            rec = None
        if rec:
            # Expect get_key_record_for_company to return either the API key string or a record dict/tuple.
            api_key = rec if isinstance(rec, str) else (rec.get("api_key") if isinstance(rec, dict) else (rec[1] if len(rec)>0 else None))
            log_event("INFO", "/register", "existing_key_returned", company=company_safe)
        else:
            api_key = generate_api_key()
            create_api_key(company_safe, api_key, daily_limit=500)
            log_event("INFO", "/register", "new_key_created", company=company_safe)
    except Exception as e:
        logger.exception("auth module helper failed: %s", e)
        # Require the auth module to be properly implemented (CloudSQL). Fail fast.
        raise HTTPException(status_code=500, detail="Auth backend not configured")

    # Build base URLs from request.base_url (keeps http/https and host)
    base = str(request.base_url).rstrip("/")  # e.g. "http://127.0.0.1:8000" or dev URL

    # Example endpoints for this API key
    examples = {
        "register_get": f"{base}/register",
        "files_list_html": f"{base}/api/v1/{company_safe}/surveys/Survey123/Question1/files/list?api_key={api_key}",
        "files_json": f"{base}/api/v1/{company_safe}/surveys/Survey123/Question1/files",
        "upload": f"{base}/api/v1/{company_safe}/surveys/Survey123/Question1/upload",
        "download": f"{base}/api/v1/{company_safe}/surveys/Survey123/Question1/download/user1_image.jpg",
        "optimize": f"{base}/api/v1/{company_safe}/surveys/Survey123/Question1/optimize/user1_image.jpg",
    }

    return templates.TemplateResponse("register_result.html", {
        "request": request,
        "company": company_safe,
        "api_key": api_key,
        "examples": examples,
        "base_url": base
    })

# http://127.0.0.1:8000/api/v1/walr/surveys/survey123/upload
@app.post("/api/v1/{company}/surveys/{survey}/{question}/upload")
async def upload_file(
    company: str,
    survey: str,
    question: str,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    filename: Optional[str] = Form(None),
    auth=Depends(verify_api_key),
):
    """
    Upload -> GCS-only
    Object path: {company}/{survey}/{question}/{user_safe}_{filename}
    """
    if not survey or not user_id or not question:
        raise HTTPException(status_code=400, detail="survey, question and user_id are required")

    logger.info("UPLOAD_REQUEST: company=%s survey=%s question=%s user_id=%s filename_field=%s content_type=%s",
                company, survey, question, user_id, filename, file.content_type)

    contents = await file.read()
    size = len(contents)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_SIZE} bytes)")

    content_type = (file.content_type or "").lower()
    if ALLOWED_PREFIXES and not any(content_type.startswith(p) for p in ALLOWED_PREFIXES):
        raise HTTPException(status_code=400, detail=f"Unsupported content type: {content_type}")

    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    question_safe = secure_name(question).lower()
    user_safe = secure_name(user_id)
    desired_name = secure_name(filename) if filename else secure_name(file.filename or "upload")

    if "." not in desired_name and "/" in content_type:
        subtype = content_type.split("/")[-1]
        desired_name = f"{desired_name}.{subtype}"
    chosen_name = f"{user_safe}_{desired_name}"

    # Save to GCS via adapter (expect save_file_bytes(company, survey_or_prefix, filename, bytes))
    # We pass survey_or_prefix as f"{survey_safe}/{question_safe}" to let adapter build object path.
    try:
        stored_path = save_file_bytes(company_safe, f"{survey_safe}/{question_safe}", chosen_name, contents)
        log_event("INFO", "/api/v1/upload", f"saved:{stored_path}", company=company_safe, survey=survey_safe, filename=chosen_name)
    except Exception as e:
        logger.exception("save_file_bytes failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save file to storage")

    # Build download signed URL; adapter signature unchanged but pass the same prefix
    try:
        # download_url = get_signed_url(company_safe, f"{survey_safe}/{question_safe}", Path(chosen_name).name)
        download_url = get_signed_url(company_safe, f"{survey_safe}/{question_safe}", Path(chosen_name).name, response_disposition=f'attachment; filename="{Path(chosen_name).name}"')
    except Exception as e:
        logger.exception("get_signed_url failed: %s", e)
        download_url = stored_path

    resp = {
        "ok": True,
        "saved_path": stored_path,
        "filename": Path(chosen_name).name,
        "size": size,
        "content_type": content_type,
        "download_url": download_url
    }
    # tags: pass full gs path if adapter returned one; otherwise build it
    gs_path = stored_path if str(stored_path).startswith("gs://") else f"gs://{GCS_BUCKET}/{company_safe}/{survey_safe}/{question_safe}/{Path(chosen_name).name}"
    add_random_tags_for_file(gs_path)
    return JSONResponse(resp)


#http://127.0.0.1:8000/files/mysurvey
#http://127.0.0.1:8000/api/v1/walr/surveys/Survey123/files
@app.get("/api/v1/{company}/surveys/{survey}/{question}/files")
async def files_json(
    company: str,
    survey: str,
    question: str,
    request: Request,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    auth=Depends(verify_api_key)
):
    """
    List objects under prefix: {company}/{survey}/{question}/
    """
    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    question_safe = secure_name(question).lower()

    if not GCS_BUCKET:
        logger.error("GCS_BUCKET not configured")
        raise HTTPException(status_code=500, detail="Server misconfigured")

    client = storage.Client()
    prefix = f"{company_safe}/{survey_safe}/{question_safe}/"
    blobs = client.list_blobs(GCS_BUCKET, prefix=prefix)
    all_blobs = [b for b in blobs if not b.name.endswith("/") and "/optimized/" not in b.name]

    # optional user filter
    if user_id:
        user_safe = secure_name(user_id)
        all_blobs = [b for b in all_blobs if Path(b.name).name.startswith(f"{user_safe}_")]

    # sort by updated desc
    all_blobs.sort(key=lambda b: b.updated or datetime.now(timezone.utc), reverse=True)
    sliced = all_blobs[offset: offset + limit]

    files_info = []
    for blob in sliced:
        name = Path(blob.name).name
        ext = Path(name).suffix.lower()
        is_video = ext in VIDEO_EXTS
        size = blob.size or 0
        modified = (blob.updated or datetime.now(timezone.utc)).astimezone().isoformat()
        try:
            download_url = get_signed_url(company_safe, f"{survey_safe}/{question_safe}", name)
        except Exception:
            download_url = f"gs://{GCS_BUCKET}/{blob.name}"
        tags = get_tags(f"gs://{GCS_BUCKET}/{blob.name}") or []
        files_info.append({
            "filename": name,
            "size": size,
            "modified": modified,
            "relative": f"gs://{GCS_BUCKET}/{blob.name}",
            "download_url": download_url,
            "tags": tags,
            "is_video": is_video
        })

    return files_info


def _probe_video_width(path: Path):
    """Return integer width (pixels) or None if probe fails."""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width",
            "-of", "csv=p=0",
            str(path)
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        out = out.strip()
        return int(out) if out else None
    except Exception:
        return None

import tempfile
from google.cloud import storage as gcs_storage
def optimize_media_and_cache(company_safe: str, survey_safe: str, question_safe: str, filename: str, target_img_width: int = 900, max_video_width: int = 1280):
    """
    GCS-only: download {company}/{survey}/{question}/{filename} -> optimize -> upload to {company}/{survey}/{question}/optimized/{opt_name}
    Returns signed URL or gs:// fallback.
    """
    bucket_name = GCS_BUCKET or os.getenv("GCS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET env var not set")

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)

    src_obj = f"{company_safe}/{survey_safe}/{question_safe}/{filename}"
    blob = bucket.blob(src_obj)
    if not blob.exists():
        # fallback: try legacy path without question (optional)
        legacy_obj = f"{company_safe}/{survey_safe}/{filename}"
        legacy_blob = bucket.blob(legacy_obj)
        if legacy_blob.exists():
            blob = legacy_blob
        else:
            raise FileNotFoundError("source not found")

    stem = Path(filename).stem
    ext = Path(filename).suffix.lower()
    opt_name = f"opt_{stem}.mp4" if ext in VIDEO_EXTS else f"opt_{stem}.jpg"
    opt_obj = f"{company_safe}/{survey_safe}/{question_safe}/optimized/{opt_name}"
    opt_blob = bucket.blob(opt_obj)

    # short-circuit if already optimized
    if opt_blob.exists():
        try:
            # return get_signed_url(company_safe, f"{survey_safe}/{question_safe}/optimized", opt_name)
            return get_signed_url(
                company_safe,
                f"{survey_safe}/{question_safe}/optimized",
                opt_name,
                response_disposition=f'attachment; filename="{opt_name}"'
            )
        except Exception:
            return f"gs://{bucket_name}/{opt_obj}"

    with tempfile.TemporaryDirectory() as td:
        local_src = Path(td) / filename
        local_out = Path(td) / opt_name
        blob.download_to_filename(str(local_src))

        if ext not in VIDEO_EXTS:
            with Image.open(local_src) as im:
                im = im.convert("RGB")
                w, h = im.size
                if w > target_img_width:
                    new_h = int((target_img_width / w) * h)
                    im = im.resize((target_img_width, new_h), Image.LANCZOS)
                im.save(local_out, format="JPEG", quality=78, optimize=True)
        else:
            src_width = _probe_video_width(local_src)
            vf = None
            if src_width and src_width > max_video_width:
                vf = f"scale={max_video_width}:-2"
            ffmpeg_cmd = ["ffmpeg", "-y", "-i", str(local_src)]
            if vf:
                ffmpeg_cmd += ["-vf", vf]
            ffmpeg_cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-c:a", "aac", "-b:a", "96k", str(local_out)]
            try:
                subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
            except FileNotFoundError:
                raise RuntimeError("ffmpeg not found on server. Install ffmpeg to enable video optimization.")
            except subprocess.CalledProcessError as e:
                stderr = (e.stderr or "")[:2000]
                raise RuntimeError(f"ffmpeg failed: {stderr}")

        # upload optimized artifact
        opt_blob.upload_from_filename(str(local_out))
        try:
            return get_signed_url(company_safe, f"{survey_safe}/{question_safe}/optimized", opt_name)
        except Exception:
            return f"gs://{bucket_name}/{opt_obj}"



#http://127.0.0.1:8000/optimize/mysurvey/filename.jpg
@app.get("/api/v1/{company}/surveys/{survey}/{question}/optimize/{filename}")
async def optimize_endpoint(company: str, survey: str, question: str, filename: str):
    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    question_safe = secure_name(question).lower()

    try:
        download_path = optimize_media_and_cache(company_safe, survey_safe, question_safe, filename)
        if isinstance(download_path, str) and (download_path.startswith("http://") or download_path.startswith("https://")):
            return JSONResponse({"ok": True, "optimized": download_path})
        elif isinstance(download_path, str) and download_path.startswith("gs://"):
            opt_name = Path(download_path).name
            try:
                signed = get_signed_url(company_safe, f"{survey_safe}/{question_safe}/optimized", opt_name)
                return JSONResponse({"ok": True, "optimized": signed})
            except Exception:
                return JSONResponse({"ok": True, "optimized": download_path})
        else:
            return JSONResponse({"ok": False, "error": "optimize produced no url", "raw": str(download_path)}, status_code=500)
    except FileNotFoundError:
        return JSONResponse({"ok": False, "error": "source file not found"}, status_code=404)
    except RuntimeError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    except Exception as e:
        logger.exception("optimize unexpected error: %s", e)
        return JSONResponse({"ok": False, "error": "unexpected error", "detail": str(e)}, status_code=500)



# Simple HTML view to list files for a survey with preview and actions
#http://127.0.0.1:8000/api/v1/walr/surveys/Survey123/files/list
@app.get("/api/v1/{company}/surveys/{survey}/{question}/files/list", response_class=HTMLResponse)
async def files_list_template(
    request: Request,
    company: str,
    survey: str,
    question: str,
    api_key: str = Query(..., alias="api_key"),
):
    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    question_safe = secure_name(question).lower()

    # validate API key
    rec = get_key_record(api_key)
    if not rec:
        raise HTTPException(status_code=401, detail="Invalid API key")
    rec_company = secure_name(rec[0])
    if rec_company != company_safe:
        raise HTTPException(status_code=403, detail="API key not valid for this company")

    files = []
    if not GCS_BUCKET:
        log_event("ERROR", "files_list_template", "GCS bucket not configured")
        return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "question": question_safe, "files": []})

    client = storage.Client()
    prefix = f"{company_safe}/{survey_safe}/{question_safe}/"
    try:
        blobs = client.list_blobs(GCS_BUCKET, prefix=prefix)
    except Exception as e:
        log_event("ERROR", "/files/list", f"gcs_list_error:{e}", company=company_safe, survey=survey_safe)
        return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "question": question_safe, "files": []})

    for blob in blobs:
        if blob.name.endswith("/") or "/optimized/" in blob.name:
            continue
        name = Path(blob.name).name
        ext = Path(name).suffix.lower()
        is_video = ext in VIDEO_EXTS
        size_kb = round((blob.size or 0) / 1024, 1)
        modified_dt = blob.updated or datetime.now(timezone.utc)
        modified = modified_dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        try:
            download_url = get_signed_url(company_safe, f"{survey_safe}/{question_safe}", name)
        except Exception:
            download_url = ""
        optimize_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/{question_safe}/optimize/{name}"
        preview_url = download_url
        try:
            gs_path = f"gs://{GCS_BUCKET}/{blob.name}"
            tags = get_tags(gs_path)
        except Exception:
            tags = []
        files.append({
            "filename": name,
            "size_kb": size_kb,
            "modified": modified,
            "download_url": download_url,
            "optimize_url": optimize_url,
            "preview_url": preview_url,
            "is_video": is_video,
            "tags": tags,
            "question": question_safe
        })

    files.sort(key=lambda f: f["modified"], reverse=True)
    return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "question": question_safe, "files": files})


@app.get("/api/v1/{company}/surveys/{survey}/{question}/download/{path:path}")
async def download_file(company: str, survey: str, question: str, path: str):
    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    question_safe = secure_name(question).lower()

    log_event("INFO", "/api/v1/{company}/surveys/{survey}/download", "download_request",
              company=company_safe, survey=survey_safe, filename=path)

    if not GCS_BUCKET:
        raise HTTPException(status_code=500, detail="Server misconfigured")

    # prefer storage_adapter helper
    try:
        # signed_url = get_signed_url(company_safe, f"{survey_safe}/{question_safe}", Path(path).name)
        signed_url = get_signed_url(
            company_safe,
            f"{survey_safe}/{question_safe}",
            Path(path).name,
            response_disposition=f'attachment; filename="{Path(path).name}"'
        )
        return RedirectResponse(url=signed_url, status_code=302)
    
    except Exception as e:
        logger.exception("get_signed_url failed: %s", e)
        # fallback: try blob existence under question path then legacy path
        client = storage.Client()
        blob = client.bucket(GCS_BUCKET).blob(f"{company_safe}/{survey_safe}/{question_safe}/{path}")
        if not blob.exists():
            # try legacy (no question)
            legacy_blob = client.bucket(GCS_BUCKET).blob(f"{company_safe}/{survey_safe}/{path}")
            if not legacy_blob.exists():
                raise HTTPException(status_code=404, detail="file not found")
            else:
                # attempt sign legacy
                try:
                    signed = legacy_blob.generate_signed_url(version="v4", expiration=timedelta(minutes=15), method="GET")
                    signed = legacy_blob.generate_signed_url(
                        version="v4",
                        expiration=timedelta(minutes=15),
                        method="GET",
                        response_disposition=f'attachment; filename="{Path(path).name}"'
                    )
                    return RedirectResponse(url=signed, status_code=302)
                except Exception:
                    return JSONResponse({"ok": True, "url": f"gs://{GCS_BUCKET}/{company_safe}/{survey_safe}/{path}"})
        # if blob exists under question but signing failed earlier, return gs:// fallback
        return JSONResponse({"ok": True, "url": f"gs://{GCS_BUCKET}/{company_safe}/{survey_safe}/{question_safe}/{path}"})
