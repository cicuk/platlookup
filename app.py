import os
import time
import pandas as pd
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

CSV_PATH = os.getenv("CSV_PATH", "platnomor.csv")
CSV_DELIMITER = ";"  # matches your file

app = FastAPI(title="CSV Lookup (Plat Nomor)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

_df_cache = None
_df_mtime = 0.0
_df_error: Optional[str] = None  # keep last load error

def _load_df():
    """Load and normalize the CSV into a pandas DataFrame."""
    global _df_error
    _df_error = None
    try:
        df = pd.read_csv(
            CSV_PATH,
            delimiter=CSV_DELIMITER,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8"
        )
    except Exception as e:
        _df_error = f"Failed to read CSV: {e}"
        return None

    if df is None or df.empty:
        _df_error = "CSV loaded but is empty."
        return None

    # We need at least 3 columns: [first, provinsi, kota]
    if df.shape[1] < 3:
        _df_error = f"CSV must have at least 3 columns, found {df.shape[1]}."
        return None

    # Normalize whitespace ONLY on string columns (object/string dtypes)
    str_cols = df.select_dtypes(include=["object"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip()

    return df

def _ensure_df():
    """Reload when file changes; cache otherwise."""
    global _df_cache, _df_mtime, _df_error
    try:
        mtime = os.path.getmtime(CSV_PATH)
    except FileNotFoundError:
        _df_cache = None
        _df_mtime = 0.0
        _df_error = f"CSV file not found: {CSV_PATH}"
        return

    if _df_cache is None or mtime != _df_mtime:
        df = _load_df()
        if df is not None:
            _df_cache = df
            _df_mtime = mtime

def search_csv(input_text: str):
    """
    Case-insensitive exact match on first column.
    Returns (kota, provinsi) where kota is 3rd col (index 2) and prov is 2nd col (index 1).
    """
    _ensure_df()
    if _df_cache is None or _df_cache.empty:
        return None, None

    # Defensive: ensure we have at least 3 columns
    if _df_cache.shape[1] < 3:
        return None, None

    first_col_series = _df_cache.iloc[:, 0].astype(str).str.strip().str.lower()
    q = (input_text or "").strip().lower()
    matches = _df_cache[first_col_series == q]

    if not matches.empty:
        kota = matches.iloc[0, 2]  # 3rd column
        prov = matches.iloc[0, 1]  # 2nd column
        return kota, prov
    return None, None

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, q: Optional[str] = Query(default=None)):
    kota, prov = (None, None)
    not_found = False
    _ensure_df()

    if q is not None and q.strip() != "":
        kota, prov = search_csv(q)
        not_found = kota is None

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "q": q or "",
            "kota": kota,
            "prov": prov,
            "not_found": not_found,
            "csv_path": CSV_PATH,
            "updated": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_df_mtime)) if _df_mtime else "—",
            "load_error": _df_error,
        },
    )

@app.get("/api/lookup")
async def api_lookup(q: str = Query(..., description="Key to search (first column)")):
    if not q.strip():
        return JSONResponse({"error": "Empty query"}, status_code=400)
    kota, prov = search_csv(q)
    if kota is None:
        return JSONResponse({"found": False, "kota": None, "provinsi": None}, status_code=404)
    return {"found": True, "kota": kota, "provinsi": prov}

@app.get("/api/reload")
async def api_reload():
    """Force a reload (optional). Auto-reload also happens on file change."""
    global _df_cache, _df_mtime
    _df_cache = None
    _df_mtime = 0.0
    _ensure_df()
    if _df_cache is None:
        return JSONResponse({"ok": False, "message": f"Failed to load {CSV_PATH}"}, status_code=500)
    return {"ok": True, "rows": int(_df_cache.shape[0]), "updated": _df_mtime}

@app.get("/api/health")
async def api_health():
    _ensure_df()
    return {
        "ok": _df_cache is not None and not _df_cache.empty,
        "rows": int(_df_cache.shape[0]) if _df_cache is not None else 0,
        "error": _df_error,
        "csv": CSV_PATH,
        "mtime": _df_mtime,
    }
