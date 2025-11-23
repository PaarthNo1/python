# main.py
from fastapi.middleware.cors import CORSMiddleware
import os
import io
import json
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

# internal imports
import services.faiss_service as faiss_service
import services.db_service as db_service
from utils.plots import plot_profile
from services.sql_ai_gemini.main import nl_to_sql_and_execute
  # 👈 NEW IMPORT

load_dotenv()

app = FastAPI(title="OceanIQ Phase3 API", version="0.1")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Optional OpenAI support
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if OPENAI_API_KEY:
    import openai
    openai.api_key = OPENAI_API_KEY


# ========= MODELS ============
class SearchRequest(BaseModel):
    query: str
    top_k: Optional[int] = 5

class GeoSearchRequest(BaseModel):
    lat: float
    lon: float
    radius_km: Optional[float] = 200.0
    query: Optional[str] = None
    top_k: Optional[int] = 10

class ChatRequest(BaseModel):
    query: str
    top_k: Optional[int] = 3

# class NLQuery(BaseModel):                # 👈 NEW MODEL
#     question: str
class NLQuery(BaseModel):
    question: str
    use_rag: Optional[bool] = False
    top_k: Optional[int] = 5


# ========= ROUTES ============

@app.get("/")
def root():
    return {"status": "ok", "msg": "OceanIQ Phase3 API running"}


@app.post("/search")
def search(req: SearchRequest):
    res = faiss_service.semantic_search(req.query, top_k=req.top_k)
    return JSONResponse(content=res)


@app.post("/geo_search")
def geo_search(req: GeoSearchRequest):
    fn = getattr(faiss_service, "geo_search", None) or getattr(faiss_service, "geo_semantic_search", None)
    if fn is None:
        raise HTTPException(status_code=500, detail="Geo search function not available.")
    return JSONResponse(content=fn(
        req.lat, req.lon, radius_km=req.radius_km, text_query=req.query, top_k=req.top_k
    ))


@app.get("/profile/{float_id}/{cycle}")
def get_profile(float_id: str, cycle: int):
    meta = db_service.get_profile_metadata(float_id, cycle)
    if not meta:
        raise HTTPException(status_code=404, detail="Profile not found")

    profile_number = meta["profile_number"]
    df = db_service.get_profile_measurements(float_id, cycle, profile_number)

    try:
        measurements = json.loads(df.to_json(orient="records", date_format="iso"))
    except:
        measurements = df.where(pd.notnull(df), None).to_dict(orient="records")

    return {"metadata": meta, "measurements": measurements}


@app.get("/plot/profile/{float_id}/{cycle}")
def plot_profile_endpoint(float_id: str, cycle: int, plot_type: Optional[str] = Query("temp", regex="^(temp|sal|both)$")):
    meta = db_service.get_profile_metadata(float_id, cycle)
    if not meta:
        raise HTTPException(status_code=404, detail="Profile not found")

    df = db_service.get_profile_measurements(float_id, cycle, meta["profile_number"])
    if df.empty:
        raise HTTPException(status_code=404, detail="Measurements not found")

    png = plot_profile(df, plot_type)
    data = png.getvalue() if hasattr(png, "getvalue") else bytes(png)

    return StreamingResponse(io.BytesIO(data), media_type="image/png")


@app.post("/chat")
def chat(req: ChatRequest):
    results = faiss_service.semantic_search(req.query, top_k=req.top_k)
    if not results:
        return {"answer": "No relevant profiles found.", "sources": []}

    summaries = [r["summary"] for r in results]
    sources = [r["uid"] for r in results]
    combined = "\n\n".join(summaries)

    if OPENAI_API_KEY:
        try:
            resp = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "Ocean data assistant."},
                    {"role": "user", "content": f"User Query: {req.query}\n\nProfiles:\n{combined}"}
                ],
                max_tokens=400,
                temperature=0.1,
            )
            return {"answer": resp["choices"][0]["message"]["content"], "sources": sources}
        except Exception as e:
            return {"answer": combined, "sources": sources, "note": str(e)}

    return {"answer": combined, "sources": sources}


# ========= NATURAL LANGUAGE → SQL (GEMINI) ============
# @app.post("/nl_query")
# def run_nl_query(req: NLQuery):
#     try:
#         result = nl_to_sql_and_execute(req.question)
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))
@app.post("/nl_query")
def run_nl_query(req: NLQuery):
    try:
        result = nl_to_sql_and_execute(req.question, use_rag=bool(req.use_rag), top_k=int(req.top_k or 5))
        return result
    except Exception as e:
        # Return safe error message; log details are in service logs
        raise HTTPException(status_code=400, detail=str(e))
