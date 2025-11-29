# main.py
from dotenv import load_dotenv, find_dotenv
load_dotenv(override=True)
print("[dotenv] loaded:", find_dotenv())

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional
import io
import json
import pandas as pd

# internal imports
# import services.faiss_service as faiss_service
import services.db_service as db_service
# from utils.plots import plot_profile
from services.sql_ai_gemini.main import nl_to_sql_and_execute  # Gemini (RAG always on)

app = FastAPI(title="OceanIQ Phase3 API", version="0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========= MODELS ============
# class SearchRequest(BaseModel):
#     query: str
#     top_k: Optional[int] = 5

class GeoSearchRequest(BaseModel):
    lat: float
    lon: float
    top_k: Optional[int] = 10

class NLQuery(BaseModel):
    question: str
    top_k: Optional[int] = 5  # NOTE: RAG is always on; no use_rag flag

# ========= ROUTES ============
@app.get("/")
def root():
    return {"status": "ok", "msg": "OceanIQ Phase3 API running"}

# @app.post("/search")
# def search(req: SearchRequest):
#     res = faiss_service.semantic_search(req.query, top_k=req.top_k)
#     return JSONResponse(content=res)

@app.post("/nearest_floats")
def nearest_floats(req: GeoSearchRequest):
    """
    Find floats near a specific location.
    """
    try:
        results = db_service.get_nearest_floats(req.lat, req.lon, req.top_k)
        return JSONResponse(content=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/float_location/{float_id}/{cycle}")
def float_location(float_id: str, cycle: int):
    """
    Get the location of a specific float cycle, including details and sensor list.
    """
    try:
        location = db_service.get_float_location(float_id, cycle)
        if not location:
            raise HTTPException(status_code=404, detail="Float location not found")
        return location
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# @app.get("/profile/{float_id}/{cycle}")
# def get_profile(float_id: str, cycle: int):
#     meta = db_service.get_profile_metadata(float_id, cycle)
#     if not meta:
#         raise HTTPException(status_code=404, detail="Profile not found")

#     df = db_service.get_profile_measurements(float_id, cycle, meta["profile_number"])

#     try:
#         measurements = json.loads(df.to_json(orient="records", date_format="iso"))
#     except Exception:
#         measurements = df.where(pd.notnull(df), None).to_dict(orient="records")

#     return {"metadata": meta, "measurements": measurements}

# @app.get("/plot/profile/{float_id}/{cycle}")
# def plot_profile_endpoint(float_id: str, cycle: int, plot_type: Optional[str] = Query("temp", regex="^(temp|sal|both)$")):
#     meta = db_service.get_profile_metadata(float_id, cycle)
#     if not meta:
#         raise HTTPException(status_code=404, detail="Profile not found")

#     df = db_service.get_profile_measurements(float_id, cycle, meta["profile_number"])
#     if df.empty:
#         raise HTTPException(status_code=404, detail="Measurements not found")

#     png = plot_profile(df, plot_type)
#     data = png.getvalue() if hasattr(png, "getvalue") else bytes(png)

#     return StreamingResponse(io.BytesIO(data), media_type="image/png")

# ========= NATURAL LANGUAGE → SQL (Gemini, RAG always on) ============
@app.post("/nl_query")
def run_nl_query(req: NLQuery):
    print(req)
    try:
        # print("hii")
        result = nl_to_sql_and_execute(req.question, top_k=int(req.top_k or 5))  # RAG forced inside
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
