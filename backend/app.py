# app.py
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi import Request
import aiofiles
import re
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from typing import List, Optional
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from PIL import Image
from tags_util import add_random_tags_for_file
import io
from fastapi.templating import Jinja2Templates


app = FastAPI(title="Upload Service")

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
VIDEO_EXTS = {'.mp4', '.webm', '.ogg', '.mov', '.m4v'}

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

# http://127.0.0.1:8000/upload
@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    survey: str = Form(...),
    user_id: str = Form(...),
    filename: Optional[str] = Form(None),
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
    survey_safe = secure_name(survey)
    user_safe = secure_name(user_id)
    desired_name = secure_name(filename) if filename else secure_name(file.filename or "upload")
    # ensure extension is present; if not, try to use subtype from content_type
    if "." not in desired_name and "/" in content_type:
        subtype = content_type.split("/")[-1]
        desired_name = f"{desired_name}.{subtype}"

    # create target dir and save file
    target_dir = ensure_dir(BASE_UPLOAD_DIR / survey_safe)
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

    # Optionally, add random tags for the uploaded file (for demo purposes)
    relative_path = str(Path("uploads") / survey_safe / target_path.name)
    add_random_tags_for_file(relative_path)

    resp = {
        "ok": True,
        "saved_path": str(target_path),
        "relative": relative_path,
        "filename": target_path.name,
        "size": size,
        "content_type": content_type
    }
    return JSONResponse(resp)

#http://127.0.0.1:8000/files/mysurvey
@app.get("/files/{survey}")
async def list_files_for_survey(
    survey: str,
    request: Request,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    List files under uploads/{survey}.
    Optional query params:
      - user_id: filter by user prefix (user_safe_)
      - limit, offset: simple pagination
    """
    survey_safe = secure_name(survey)
    target_dir = BASE_UPLOAD_DIR / survey_safe

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
        rel_path = str(Path(survey_safe) / p.name)
        try:
            download_url = request.url_for("uploads", path=rel_path)
        except Exception:
            # fallback: construct absolute URL manually
            base = str(request.base_url).rstrip("/")
            download_url = f"{base}/uploads/{rel_path}"
        files_info.append({
            "filename": p.name,
            "size": st.st_size,
            "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            "relative": str(Path("uploads") / survey_safe / p.name),
            "download_url": download_url
        })

    return files_info

def optimize_image_and_cache(survey_safe: str, filename: str, width: int = 900):
    """
    - Reads uploads/{survey_safe}/{filename}
    - Creates uploads/{survey_safe}/optimized/{filename} (JPEG or WebP)
    - Returns the relative path to optimized file
    """

    src = BASE_UPLOAD_DIR / survey_safe / filename
    if not src.exists():
        raise FileNotFoundError("source not found")

    opt_dir = ensure_dir(BASE_UPLOAD_DIR / survey_safe / "optimized")
    # choose output name; keep extension .jpg (convert if needed)
    out_name = f"opt_{Path(filename).stem}.jpg"
    out_path = opt_dir / out_name

    # if already exists, return quickly
    if out_path.exists():
        return str(Path("uploads") / survey_safe / "optimized" / out_name)

    # open and resize
    with Image.open(src) as im:
        im = im.convert("RGB")
        # maintain aspect ratio
        w, h = im.size
        if w > width:
            new_h = int((width / w) * h)
            im = im.resize((width, new_h), Image.LANCZOS)
        # save optimized
        im.save(out_path, format="JPEG", quality=78, optimize=True)
    return str(Path("uploads") / survey_safe / "optimized" / out_name)


#http://127.0.0.1:8000/optimize/mysurvey/filename.jpg
@app.get("/optimize/{survey}/{filename}")
async def optimize_endpoint(survey: str, filename: str):
    survey_safe = secure_name(survey)
    try:
        rel = optimize_image_and_cache(survey_safe, filename)
    except FileNotFoundError:
        return JSONResponse({"ok": False, "error": "source file not found"}, status_code=404)
    # return JSON with optimized relative path (served by static mount)
    return {"ok": True, "optimized": rel}


# Simple HTML view to list files for a survey with preview and actions
#http://127.0.0.1:8000/files/mysurvey/list
VIDEO_EXTS = {'.mp4', '.webm', '.ogg', '.mov', '.m4v'}

@app.get("/files/{survey}/list", response_class=HTMLResponse)
async def files_list_template(request: Request, survey: str):
    survey_safe = secure_name(survey)
    target_dir = BASE_UPLOAD_DIR / survey_safe
    if not target_dir.exists():
        return templates.TemplateResponse("files_list.html", {"request": request, "survey": survey_safe, "files": []})

    files = []
    for p in sorted(target_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if not p.is_file(): 
            continue
        name = p.name
        ext = p.suffix.lower()
        is_video = ext in VIDEO_EXTS
        size_kb = round(p.stat().st_size / 1024, 1)
        modified = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        # download URL (served by static mount /uploads)
        download_url = f"/uploads/{survey_safe}/{name}"
        # optimized endpoint (on-demand)
        optimize_url = f"/optimize/{survey_safe}/{name}"
        preview_url = download_url  # for images use original; for video the video tag consumes this
        # tags (if any)
        try:
            from tags_util import get_tags
            tags = get_tags(str(Path("uploads") / survey_safe / name))
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
    return templates.TemplateResponse("files_list.html", {"request": request, "survey": survey_safe, "files": files})


