# app.py
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi import Request
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from PIL import Image
from tags_util import add_random_tags_for_file, get_tags
from fastapi.responses import FileResponse
from fastapi import Header, Depends
from auth import get_key_record, create_api_key, increment_usage_and_check, init_db as auth_init_db
from fastapi import HTTPException, status
from logs_util import log_event, init_logs_db
import subprocess
import secrets
import aiofiles
import re
import io
import mimetypes
import os
from storage_adapter import save_file_bytes, save_file_from_path, get_signed_url, USE_GCS
from google.cloud import storage
from typing import List

from pathlib import Path

# ensure DB directory is used for sqlite files (persisted via volume)
DB_DIR = Path("uploads")
DB_DIR.mkdir(parents=True, exist_ok=True)

# (optional) read env locally
GCS_ENABLED = USE_GCS


app = FastAPI(title="Upload Service")

# initialize auth DB (dev)
auth_init_db()

# intialize logs DB
init_logs_db()

# mount static + templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# development helper: allow local origins; replace '*' with specific origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # or ["http://localhost:5173", "http://127.0.0.1:5500"]
    allow_credentials=True,
    allow_methods=["*"],            # allow OPTIONS, POST, GET, etc.
    allow_headers=["*"],            # allow Content-Type, Authorization, etc.
)

# Config
BASE_UPLOAD_DIR = Path("uploads")
BASE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB max (adjust if needed)
ALLOWED_PREFIXES = ("image/", "video/")  # optional restriction; set to () to allow any
VIDEO_EXTS = {'.mp4', '.webm', '.ogg', '.mov', '.m4v', '.avi', '.flv', '.mkv'}








import logging, os
logger = logging.getLogger("filetagapi")
logging.basicConfig(level=logging.INFO)

# Log startup env snapshot
logger.info("STARTUP: USE_GCS (env)=%s GCS_BUCKET=%s GOOGLE_APPLICATION_CREDENTIALS=%s CWD=%s",
            os.getenv("USE_GCS"), os.getenv("GCS_BUCKET"), os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), os.getcwd())





# mount the uploads folder so files are served at /uploads/...
app.mount("/uploads", StaticFiles(directory=str(BASE_UPLOAD_DIR)), name="uploads")

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
            api_key = rec
            log_event("INFO", "/register", "existing_key_returned", company=company_safe)
        else:
            api_key = generate_api_key()
            create_api_key(company_safe, api_key, daily_limit=500)
            log_event("INFO", "/register", "new_key_created", company=company_safe)
    except Exception:
        # fallback to raw sqlite access (backwards-compatible)
        import sqlite3
        con = sqlite3.connect(str(DB_DIR / "auth.db"))
        cur = con.cursor()
        cur.execute("SELECT api_key FROM api_keys WHERE company = ?", (company_safe,))
        row = cur.fetchone()
        if row:
            api_key = row[0]
            log_event("INFO", "/register", "existing_key_returned", company=company_safe)
        else:
            api_key = generate_api_key()
            # try to use create_api_key, else insert raw
            try:
                create_api_key(company_safe, api_key, daily_limit=500)
            except Exception:
                cur.execute("INSERT INTO api_keys (company, api_key, daily_limit) VALUES (?, ?, ?)",
                            (company_safe, api_key, 500))
                con.commit()
            log_event("INFO", "/register", "new_key_created", company=company_safe)
        con.close()

    # Build base URLs from request.base_url (keeps http/https and host)
    base = str(request.base_url).rstrip("/")  # e.g. "http://127.0.0.1:8000" or dev URL

    # Example endpoints for this API key
    examples = {
        "register_get": f"{base}/register",
        "files_list_html": f"{base}/api/v1/{company_safe}/surveys/Survey123/files/list?api_key={api_key}",
        "files_json": f"{base}/api/v1/{company_safe}/surveys/Survey123/files",
        "upload": f"{base}/api/v1/{company_safe}/surveys/Survey123/upload",
        "download": f"{base}/api/v1/{company_safe}/surveys/Survey123/download/user1_image.jpg",
        "optimize": f"{base}/api/v1/{company_safe}/surveys/Survey123/optimize/user1_image.jpg",
    }

    return templates.TemplateResponse("register_result.html", {
        "request": request,
        "company": company_safe,
        "api_key": api_key,
        "examples": examples,
        "base_url": base
    })

# http://127.0.0.1:8000/api/v1/walr/surveys/survey123/upload
@app.post("/api/v1/{company}/surveys/{survey}/upload")
async def upload_file(
    company: str,                          # path param (do NOT use Form here)
    survey: str,                           # path param (do NOT use Form here)
    file: UploadFile = File(...),          # file from multipart
    user_id: str = Form(...),              # form field inside multipart
    filename: Optional[str] = Form(None),  # optional form field
    auth=Depends(verify_api_key),          # dependency (auth) uses company
):
    
    """
    Minimal upload endpoint.
    Form fields:
      - file: binary file
      - survey: survey identifier (used as folder name)
      - user_id: user identifier (used as folder name)
      - filename: optional filename to use (otherwise uploaded filename is used)
    """
    # Basic sanity checks
    if not survey or not user_id:
        raise HTTPException(status_code=400, detail="survey and user_id are required")
    




    logger.info("UPLOAD_REQUEST: company=%s survey=%s user_id=%s filename_field=%s content_type=%s", 
                company, survey, user_id, filename, file.content_type)

    # show GCS mode as seen by storage_adapter
    try:
        import storage_adapter
        logger.info("STORAGE ADAPTER: USE_GCS=%s GCS_BUCKET=%s", getattr(storage_adapter, "USE_GCS", None), getattr(storage_adapter, "GCS_BUCKET", None))
    except Exception as e:
        logger.exception("storage_adapter import failed: %s", e)




    # Read file into memory chunk-by-chunk to check size and then write to disk
    contents = await file.read()
    size = len(contents)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_SIZE} bytes)")

    content_type = (file.content_type or "").lower()
    if ALLOWED_PREFIXES and not any(content_type.startswith(p) for p in ALLOWED_PREFIXES):
        raise HTTPException(status_code=400, detail=f"Unsupported content type: {content_type}")

    # sanitize names
    survey_safe = secure_name(survey).lower()
    user_safe = secure_name(user_id)
    desired_name = secure_name(filename) if filename else secure_name(file.filename or "upload")
    company_safe = secure_name(company).lower()
    # ensure extension is present; if not, try to use subtype from content_type
    if "." not in desired_name and "/" in content_type:
        subtype = content_type.split("/")[-1]
        desired_name = f"{desired_name}.{subtype}"

    # create a deterministic filename (userprefix + desired_name)
    chosen_name = f"{user_safe}_{desired_name}"

    # if local FS mode we still avoid overwrite by appending suffix
    if not GCS_ENABLED:
        # create target dir and save file
        target_dir = ensure_dir(BASE_UPLOAD_DIR / company_safe / survey_safe) 
        target_path = target_dir / f"{user_safe}_{desired_name}"

        # avoid accidental overwrite: if file exists, append an incrementing suffix
        counter = 1
        base = target_path.stem
        ext = target_path.suffix
        while target_path.exists():
            target_path = target_dir / f"{base}_{counter}{ext}"
            counter += 1

        async with aiofiles.open(target_path, "wb") as out_f:
            await out_f.write(contents)

        stored_path = str(target_path)
        log_event("INFO", "/api/v1/{company}/surveys/{survey}/upload", "file_uploaded", company=company_safe, survey=survey_safe, filename=target_path.name)
    else:
        stored_path = save_file_bytes(company_safe, survey_safe, chosen_name, contents)
        log_event("INFO", "/api/v1/{company}/surveys/{survey}/upload", f"saved:{stored_path}", company=company_safe, survey=survey_safe, filename=chosen_name)
    
    # Correcting response fields to reflect storage adapter
    if GCS_ENABLED:
        resp_relative = stored_path  # e.g. gs://bucket/...
        resp_download = get_signed_url(company_safe, survey_safe, Path(chosen_name).name)
    else:
        resp_relative = str(Path("uploads") / company_safe / survey_safe / Path(stored_path).name)
        # direct download route for local files:
        resp_download = f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{Path(stored_path).name}"

    resp = {
        "ok": True,
        "saved_path": stored_path,
        "relative": resp_relative,
        "filename": Path(chosen_name).name,
        "size": size,
        "content_type": content_type,
        "download_url": resp_download
    }
    add_random_tags_for_file(resp_relative)

    return JSONResponse(resp)

#http://127.0.0.1:8000/files/mysurvey
#http://127.0.0.1:8000/api/v1/walr/surveys/Survey123/files
@app.get("/api/v1/{company}/surveys/{survey}/files")
async def files_json(
    company: str,
    survey: str,
    request: Request,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    auth=Depends(verify_api_key)
):
    """
    List files under uploads/{survey}.
    Optional query params:
      - user_id: filter by user prefix (user_safe_)
      - limit, offset: simple pagination
    """
    survey_safe = secure_name(survey).lower()
    company_safe = secure_name(company).lower()
    target_dir = BASE_UPLOAD_DIR / company_safe / survey_safe
    log_event("INFO", "/api/v1/{company}/surveys/{survey}/files", "list_request", company=company_safe, survey=survey_safe)

    if not target_dir.exists() or not target_dir.is_dir():
        return []

    user_filter_prefix = None
    if user_id:
        user_safe = secure_name(user_id)
        user_filter_prefix = f"{user_safe}_"

    # gather files sorted by mtime desc
    all_files = [p for p in sorted(target_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True) if p.is_file()]

    # apply optional user filter
    if user_filter_prefix:
        all_files = [p for p in all_files if p.name.startswith(user_filter_prefix)]

    # apply pagination
    sliced = all_files[offset: offset + limit]

    files_info = []
    for p in sliced:
        st = p.stat()
        # construct a download URL using the mounted 'uploads' route
        # path must be relative to the mount; join survey_safe & filename

        # Correcting: produce download URL using storage adapter (signed URL for GCS, local endpoint otherwise)
        filename = p.name
        if GCS_ENABLED:
            # In GCS mode we expect objects stored at {company}/{survey}/{filename}
            download_url = get_signed_url(company_safe, survey_safe, filename)
            rel_path = f"gs://{os.getenv('GCS_BUCKET')}/{company_safe}/{survey_safe}/{filename}"
        else:
            # local filesystem - we can return direct download endpoint
            try:
                rel_path = str(Path("uploads") / company_safe / survey_safe / filename)
                download_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{filename}"
            except Exception:
                base = str(request.base_url).rstrip("/")
                download_url = f"{base}/uploads/{company_safe}/{survey_safe}/{filename}"
                rel_path = str(Path("uploads") / company_safe / survey_safe / filename)

        files_info.append({
            "filename": filename,
            "size": st.st_size,
            "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            "relative": rel_path,
            "download_url": download_url,
            "tags": get_tags(str(Path("uploads") / company_safe / survey_safe / filename)) if not GCS_ENABLED else get_tags(rel_path)
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

# def optimize_media_and_cache(company_safe: str, survey_safe: str, filename: str, target_img_width: int = 900, max_video_width: int = 1280, auth=Depends(verify_api_key)):
#     """
#     Optimize image OR video and cache result at:
#       uploads/{survey_safe}/optimized/opt_{stem}.jpg  (images)
#       uploads/{survey_safe}/optimized/opt_{stem}.mp4  (videos)

#     Returns a download path (starting with /download/...), or raises RuntimeError/FileNotFoundError.
#     """
#     src = BASE_UPLOAD_DIR / company_safe / survey_safe / filename
#     if not src.exists():
#         raise FileNotFoundError("source not found")

#     opt_dir = ensure_dir(BASE_UPLOAD_DIR / company_safe / survey_safe / "optimized")
#     stem = src.stem
#     ext = src.suffix.lower()

#     # IMAGE
#     if ext not in VIDEO_EXTS:
#         out_name = f"opt_{stem}.jpg"
#         out_path = opt_dir / out_name
#         if out_path.exists():
#             return f"/download/{survey_safe}/optimized/{out_name}"
#         try:
#             from PIL import Image
#             with Image.open(src) as im:
#                 im = im.convert("RGB")
#                 w, h = im.size
#                 if w > target_img_width:
#                     new_h = int((target_img_width / w) * h)
#                     im = im.resize((target_img_width, new_h), Image.LANCZOS)
#                 im.save(out_path, format="JPEG", quality=78, optimize=True)
#             return f"/download/{survey_safe}/optimized/{out_name}"
#         except Exception as e:
#             raise RuntimeError(f"image optimize failed: {e}")

#     # VIDEO
#     out_name = f"opt_{stem}.mp4"
#     out_path = opt_dir / out_name
#     if out_path.exists():
#         return f"/download/{survey_safe}/optimized/{out_name}"

#     # probe source width and decide scaling
#     src_width = _probe_video_width(src)
#     vf = None
#     if src_width and src_width > max_video_width:
#         # simple, safe scale (ensure even height with -2)
#         vf = f"scale={max_video_width}:-2"

#     ffmpeg_cmd = [
#         "ffmpeg", "-y", "-i", str(src)
#     ]
#     if vf:
#         ffmpeg_cmd += ["-vf", vf]
#     # encode to widely compatible mp4/h264+aac
#     ffmpeg_cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-c:a", "aac", "-b:a", "96k", str(out_path)]

#     try:
#         proc = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
#         return f"/download/{survey_safe}/optimized/{out_name}"
#     except FileNotFoundError:
#         raise RuntimeError("ffmpeg not found on server. Install ffmpeg to enable video optimization.")
#     except subprocess.CalledProcessError as e:
#         stderr = (e.stderr or "")[:2000]
#         raise RuntimeError(f"ffmpeg failed: {stderr}")

# Correcting: remove FastAPI dependency from helper and make returned path include company
# def optimize_media_and_cache(company_safe: str, survey_safe: str, filename: str, target_img_width: int = 900, max_video_width: int = 1280):
#     """
#     Returns a path starting with 'uploads/...' pointing at the optimized file.
#     """
#     src = BASE_UPLOAD_DIR / company_safe / survey_safe / filename
#     if not src.exists():
#         raise FileNotFoundError("source not found")

#     opt_dir = ensure_dir(BASE_UPLOAD_DIR / company_safe / survey_safe / "optimized")
#     stem = src.stem
#     ext = src.suffix.lower()

#     # IMAGE
#     if ext not in VIDEO_EXTS:
#         out_name = f"opt_{stem}.jpg"
#         out_path = opt_dir / out_name
#         if out_path.exists():
#             local_out_rel = Path("uploads") / company_safe / survey_safe / "optimized" / out_name
#             # If GCS enabled, upload optimized file and return gs path OR signed URL will be constructed by caller
#             if GCS_ENABLED:
#                 # upload local_out_rel to GCS at same object path
#                 saved = save_file_from_path(company_safe, survey_safe + "/optimized", out_name, Path(local_out_rel))
#                 # save_file_from_path returns gs://bucket/...
#                 return saved
#             else:
#                 return str(local_out_rel)
#         try:
#             from PIL import Image
#             with Image.open(src) as im:
#                 im = im.convert("RGB")
#                 w, h = im.size
#                 if w > target_img_width:
#                     new_h = int((target_img_width / w) * h)
#                     im = im.resize((target_img_width, new_h), Image.LANCZOS)
#                 im.save(out_path, format="JPEG", quality=78, optimize=True)
#             local_out_rel = Path("uploads") / company_safe / survey_safe / "optimized" / out_name
#             # If GCS enabled, upload optimized file and return gs path OR signed URL will be constructed by caller
#             if GCS_ENABLED:
#                 # upload local_out_rel to GCS at same object path
#                 saved = save_file_from_path(company_safe, survey_safe + "/optimized", out_name, Path(local_out_rel))
#                 # save_file_from_path returns gs://bucket/...
#                 return saved
#             else:
#                 return str(local_out_rel)  # Correcting the path here
#         except Exception as e:
#             raise RuntimeError(f"image optimize failed: {e}")

#     # VIDEO
#     out_name = f"opt_{stem}.mp4"
#     out_path = opt_dir / out_name
#     if out_path.exists():
#         local_out_rel = Path("uploads") / company_safe / survey_safe / "optimized" / out_name
#         # If GCS enabled, upload optimized file and return gs path OR signed URL will be constructed by caller
#         if GCS_ENABLED:
#             # upload local_out_rel to GCS at same object path
#             saved = save_file_from_path(company_safe, survey_safe + "/optimized", out_name, Path(local_out_rel))
#             # save_file_from_path returns gs://bucket/...
#             return saved
#         else:
#             return str(local_out_rel)

#     # probe source width and decide scaling
#     src_width = _probe_video_width(src)
#     vf = None
#     if src_width and src_width > max_video_width:
#         # simple, safe scale (ensure even height with -2)
#         vf = f"scale={max_video_width}:-2"

#     ffmpeg_cmd = [
#         "ffmpeg", "-y", "-i", str(src)
#     ]
#     if vf:
#         ffmpeg_cmd += ["-vf", vf]
#     ffmpeg_cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-c:a", "aac", "-b:a", "96k", str(out_path)]

#     try:
#         proc = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
#         local_out_rel = Path("uploads") / company_safe / survey_safe / "optimized" / out_name
#         # If GCS enabled, upload optimized file and return gs path OR signed URL will be constructed by caller
#         if GCS_ENABLED:
#             # upload local_out_rel to GCS at same object path
#             saved = save_file_from_path(company_safe, survey_safe + "/optimized", out_name, Path(local_out_rel))
#             # save_file_from_path returns gs://bucket/...
#             return saved
#         else:
#             return str(local_out_rel)
#     except FileNotFoundError:
#         raise RuntimeError("ffmpeg not found on server. Install ffmpeg to enable video optimization.")
#     except subprocess.CalledProcessError as e:
#         stderr = (e.stderr or "")[:2000]
#         raise RuntimeError(f"ffmpeg failed: {stderr}")

import tempfile
from google.cloud import storage as gcs_storage
def optimize_media_and_cache(company_safe: str, survey_safe: str, filename: str, target_img_width: int = 900, max_video_width: int = 1280):
    """
    GCS-aware : 
      - If GCS_ENABLED: download source blob to temp, optimize locally, upload optimized to GCS at
        {company_safe}/{survey_safe}/optimized/opt_{stem}.{ext}
      - If not GCS_ENABLED: operate on local uploads dir as before.
    Returns:
      - If GCS: either a signed URL (via get_signed_url) or the gs://... path (fallback)
      - If LOCAL: path under "uploads/..." (string)
    Raises FileNotFoundError if source missing (both local and bucket checked).
    """
    stem = Path(filename).stem
    ext = Path(filename).suffix.lower()
    opt_name = f"opt_{stem}.mp4" if ext in VIDEO_EXTS else f"opt_{stem}.jpg"

    # LOCAL MODE
    if not GCS_ENABLED:
        src = BASE_UPLOAD_DIR / company_safe / survey_safe / filename
        if not src.exists():
            raise FileNotFoundError("source not found")
        opt_dir = ensure_dir(BASE_UPLOAD_DIR / company_safe / survey_safe / "optimized")
        out_path = opt_dir / opt_name
        # If optimized exists, return relative path
        if out_path.exists():
            return str(Path("uploads") / company_safe / survey_safe / "optimized" / opt_name)
        # Continue to optimize locally (images/videos) - reuse your existing logic below.
        # --- image branch
        if ext not in VIDEO_EXTS:
            from PIL import Image
            with Image.open(src) as im:
                im = im.convert("RGB")
                w, h = im.size
                if w > target_img_width:
                    new_h = int((target_img_width / w) * h)
                    im = im.resize((target_img_width, new_h), Image.LANCZOS)
                im.save(out_path, format="JPEG", quality=78, optimize=True)
            return str(Path("uploads") / company_safe / survey_safe / "optimized" / opt_name)
        # --- video branch
        src_width = _probe_video_width(src)
        vf = None
        if src_width and src_width > max_video_width:
            vf = f"scale={max_video_width}:-2"
        ffmpeg_cmd = ["ffmpeg", "-y", "-i", str(src)]
        if vf:
            ffmpeg_cmd += ["-vf", vf]
        ffmpeg_cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-c:a", "aac", "-b:a", "96k", str(out_path)]
        try:
            subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
            return str(Path("uploads") / company_safe / survey_safe / "optimized" / opt_name)
        except FileNotFoundError:
            raise RuntimeError("ffmpeg not found on server. Install ffmpeg to enable video optimization.")
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "")[:2000]
            raise RuntimeError(f"ffmpeg failed: {stderr}")

    # GCS MODE
    # We'll: download source blob to temp, optimize locally, upload optimized back to GCS under optimized/ prefix
    bucket_name = os.getenv("GCS_BUCKET")  # ensure you set this env var
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET env var not set")

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)

    # optimized object path
    opt_obj = f"{company_safe}/{survey_safe}/optimized/{opt_name}"
    opt_blob = bucket.blob(opt_obj)

    # 1) If optimized already exists -> short-circuit and return signed URL (if available)
    if opt_blob.exists():
        try:
            signed = get_signed_url(company_safe, survey_safe + "/optimized", opt_name)
            return signed
        except Exception:
            # fallback: return gs:// path
            return f"gs://{bucket_name}/{opt_obj}"


    object_name = f"{company_safe}/{survey_safe}/{filename}"
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError("source not found")

    with tempfile.TemporaryDirectory() as td:
        local_src = Path(td) / filename
        local_out = Path(td) / opt_name

        # download source
        blob.download_to_filename(str(local_src))

        # IMAGE Optimization
        if ext not in VIDEO_EXTS:
            try:
                from PIL import Image
                with Image.open(local_src) as im:
                    im = im.convert("RGB")
                    w, h = im.size
                    if w > target_img_width:
                        new_h = int((target_img_width / w) * h)
                        im = im.resize((target_img_width, new_h), Image.LANCZOS)
                    im.save(local_out, format="JPEG", quality=78, optimize=True)
            except Exception as e:
                raise RuntimeError(f"image optimize failed: {e}")

        else:
            # VIDEO optimization via ffmpeg
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

        # upload optimized file back to GCS: target object path = {company}/{survey}/optimized/{opt_name}
        target_obj = f"{company_safe}/{survey_safe}/optimized/{opt_name}"

        # First try your storage adapter's save_file_from_path (keeps adapter abstraction)
        try:
            # save_file_from_path(company, object_path_prefix, filename, pathlib.Path)
            # Your existing usage passed survey_safe + "/optimized" earlier - keep that API shape
            saved = save_file_from_path(company_safe, survey_safe + "/optimized", opt_name, local_out)
            # `saved` should be like "gs://bucket/...." in your adapter; if adapter returns gs:// path we can get filename
            # Attempt to convert to signed URL for client consumption
            try:
                # build signed URL via adapter
                signed = get_signed_url(company_safe, survey_safe + "/optimized", opt_name)
                return signed
            except Exception:
                # fallback: return saved path or gs://... path
                return saved
        except Exception:
            # If save_file_from_path absent or fails, fallback to using the client directly
            target_blob = bucket.blob(target_obj)
            target_blob.upload_from_filename(str(local_out))
            # Make sure we return a URL the client can use; try your adapter's signed URL creator
            try:
                return get_signed_url(company_safe, survey_safe + "/optimized", opt_name)
            except Exception:
                # fallback to gs:// path
                return f"gs://{bucket_name}/{target_obj}"


#http://127.0.0.1:8000/optimize/mysurvey/filename.jpg
@app.get("/api/v1/{company}/surveys/{survey}/optimize/{filename}")
async def optimize_endpoint(company: str, survey: str, filename: str):
    survey_safe = secure_name(survey).lower()
    company_safe = secure_name(company).lower()
    try:
        log_event("INFO", "/api/v1/{company}/surveys/{survey}/optimize", f"optimize_request for {filename}", company=company_safe, survey=survey_safe, filename=filename)
        download_path = optimize_media_and_cache(company_safe, survey_safe, filename)
        # download_path is like "uploads/{company}/{survey}/optimized/opt_name"
        # - "uploads/..." (local) OR "gs://bucket/..." (GCS) OR already a signed URL
        if GCS_ENABLED:
            if download_path.startswith("gs://"):
                # convert to signed URL for client consumption
                # extract filename and build signed URL via adapter
                filename_only = Path(download_path).name
                optimized_url = get_signed_url(company_safe, survey_safe + "/optimized", filename_only)
            else:
                # fallback: assume returned is path under uploads
                optimized_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/download/optimized/{Path(download_path).name}"
        else:
            # local behavior
            if str(download_path).startswith("uploads"):
                optimized_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/download/optimized/{Path(download_path).name}"
            else:
                optimized_url = download_path

    except FileNotFoundError:
        log_event("WARN", "/api/v1/{company}/surveys/{survey}/optimize", "source_not_found", company=company_safe, survey=survey_safe, filename=filename)
        return JSONResponse({"ok": False, "error": "source file not found"}, status_code=404)
    except RuntimeError as e:
        # return helpful message and 500 so frontend can show it
        log_event("ERROR", "/api/v1/{company}/surveys/{survey}/optimize", f"opt_error: {e}", company=company_safe, survey=survey_safe, filename=filename)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    if optimized_url.startswith("http"):
        return RedirectResponse(url=optimized_url, status_code=302)    
    log_event("INFO", "/api/v1/{company}/surveys/{survey}/optimize", f"optimized_ready {optimized_url}", company=company_safe, survey=survey_safe, filename=filename)
    return {"ok": True, "optimized": optimized_url}



# Simple HTML view to list files for a survey with preview and actions
#http://127.0.0.1:8000/api/v1/walr/surveys/Survey123/files/list
@app.get("/api/v1/{company}/surveys/{survey}/files/list", response_class=HTMLResponse)
async def files_list_template(
    request: Request,
    company: str,
    survey: str,
    api_key: str = Query(..., alias="api_key"),  # require api_key in URL
):
    survey_safe = secure_name(survey).lower()
    company_safe = secure_name(company).lower()

    # validate API key
    rec = get_key_record(api_key)
    if not rec:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    rec_company = secure_name(rec[0])
    if rec_company != company_safe:
        raise HTTPException(status_code=403, detail="API key not valid for this company")
    
    files: List[dict] = []
    target_dir = BASE_UPLOAD_DIR / company_safe / survey_safe

    if GCS_ENABLED:
        # read bucket name
        GCS_BUCKET_NAME = os.getenv("GCS_BUCKET")
        if not GCS_BUCKET_NAME:
            log_event("ERROR", "files_list_template", "GCS bucket not configured")
            return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "files": []})

        client = storage.Client()  # uses ADC or GOOGLE_APPLICATION_CREDENTIALS
        prefix = f"{company_safe}/{survey_safe}/"
        try:
            blobs = client.list_blobs(GCS_BUCKET_NAME, prefix=prefix)
        except Exception as e:
            log_event("ERROR", "/api/v1/{company}/surveys/{survey}/files/list", f"gcs_list_error:{e}", company=company_safe, survey=survey_safe)
            return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "files": []})

        for blob in blobs:
            # blob.name is like "company/survey/filename.ext"
            # skip "directories"
            if blob.name.endswith("/"):
                continue
            # skip optimized objects — they live under ".../optimized/..."
            if "/optimized/" in blob.name:
                continue

            name = blob.name.split("/")[-1]
            ext = Path(name).suffix.lower()
            is_video = ext in VIDEO_EXTS

            size_kb = round((blob.size or 0) / 1024, 1)
            # blob.updated is a datetime in UTC usually
            modified_dt = blob.updated or datetime.now(timezone.utc)
            modified = modified_dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

            # download/preview URLs
            try:
                download_url = get_signed_url(company_safe, survey_safe, name)
            except Exception:
                # fallback to public URL pattern or empty
                download_url = ""

            optimize_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/optimize/{name}"
            preview_url = download_url

            # tags: pass GCS path (or adapt to whatever your get_tags expects)
            try:
                gs_path = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
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
                "tags": tags
            })

        # sort by modified desc
        files.sort(key=lambda f: f["modified"], reverse=True)
        print(files)
    else:
        if not target_dir.exists():
            return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "files": []})

        
        for p in sorted(target_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            # skip optimized directory contents
            if "optimized" in p.parts:
                continue
            if not p.is_file(): 
                continue
            name = p.name
            ext = p.suffix.lower()
            is_video = ext in VIDEO_EXTS
            size_kb = round(p.stat().st_size / 1024, 1)
            modified = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            # download URL (served by static mount /uploads)
            download_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/download/{name}"
            # optimized endpoint (on-demand)
            optimize_url = f"/api/v1/{company_safe}/surveys/{survey_safe}/optimize/{name}"
            preview_url = download_url  # for images use original; for video the video tag consumes this
            # tags (if any)
            try:
                tags = get_tags(str(Path("uploads") / company_safe / survey_safe / name)) 
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
                "tags": tags
            })
    return templates.TemplateResponse("files_list.html", {"request": request, "company": company_safe, "survey": survey_safe, "files": files})

# @app.get("/api/v1/{company}/surveys/{survey}/download/{path:path}")
# async def download_file(company: str, survey: str, path: str):
    
#     company_safe = secure_name(company).lower()
#     survey_safe = secure_name(survey).lower()
#     candidate = (BASE_UPLOAD_DIR / company_safe / survey_safe / Path(path)).resolve()
#     base_resolved = (BASE_UPLOAD_DIR / company_safe / survey_safe).resolve()
#     log_event("INFO", "/api/v1/{company}/surveys/{survey}/download", "download_request", company=company_safe, survey=survey_safe, filename=path)

#     if not str(candidate).startswith(str(base_resolved)):
#         raise HTTPException(status_code=400, detail="invalid path")
#     if not candidate.exists() or not candidate.is_file():
#         raise HTTPException(status_code=404, detail="file not found")
    
#     mimetype, _ = mimetypes.guess_type(str(candidate))

#     log_event("INFO", "/api/v1/{company}/surveys/{survey}/download", "download_served", company=company_safe, survey=survey_safe, filename=path)
    
#     return FileResponse(path=candidate, media_type=mimetype or "application/octet-stream",
#                         filename=candidate.name, headers={"Content-Disposition": f'attachment; filename="{candidate.name}"'})

@app.get("/api/v1/{company}/surveys/{survey}/download/{path:path}")
async def download_file(company: str, survey: str, path: str):
    company_safe = secure_name(company).lower()
    survey_safe = secure_name(survey).lower()
    log_event("INFO", "/api/v1/{company}/surveys/{survey}/download", "download_request", company=company_safe, survey=survey_safe, filename=path)

    if GCS_ENABLED:
        # Build object name: support nested paths like "optimized/opt_name" or plain filename
        object_path = f"{company_safe}/{survey_safe}/{path.lstrip('/')}"
        bucket_name = os.getenv("GCS_BUCKET")
        if not bucket_name:
            log_event("ERROR", "download_file", "no_gcs_bucket_configured")
            raise HTTPException(status_code=500, detail="Server misconfigured")

        try:
            client = gcs_storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_path)

            if not blob.exists():
                raise HTTPException(status_code=404, detail="file not found")

            # Desired filename in Content-Disposition (use actual file name)
            filename_only = Path(path).name

            # prefer using blob.content_type when available (fall back to generic)
            resp_type = blob.content_type if getattr(blob, "content_type", None) else "application/octet-stream"


            # Generate a V4 signed URL with Content-Disposition forcing download
            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET",
                response_disposition=f'attachment; filename="{filename_only}"',
                response_type=resp_type
            )

            # Redirect client to the signed URL so the browser downloads the file
            return RedirectResponse(url=signed_url, status_code=302)

        except HTTPException:
            raise
        except Exception as e:
            log_event("ERROR", "download_file", f"gcs_signed_url_error:{e}", company=company_safe, survey=survey_safe, filename=path)
            raise HTTPException(status_code=500, detail="Failed to generate download URL")

    # -----------------------
    # LOCAL MODE (unchanged, still returns FileResponse with attachment header)
    candidate = (BASE_UPLOAD_DIR / company_safe / survey_safe / Path(path)).resolve()
    base_resolved = (BASE_UPLOAD_DIR / company_safe / survey_safe).resolve()

    if not str(candidate).startswith(str(base_resolved)):
        raise HTTPException(status_code=400, detail="invalid path")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="file not found")

    mimetype, _ = mimetypes.guess_type(str(candidate))
    log_event("INFO", "/api/v1/{company}/surveys/{survey}/download", "download_served_local", company=company_safe, survey=survey_safe, filename=path)
    return FileResponse(path=candidate, media_type=mimetype or "application/octet-stream",
                        filename=candidate.name, headers={"Content-Disposition": f'attachment; filename="{candidate.name}"'})


