✅ FULL TEST CASE PACK — 40+ test cases

(semantic, geo, nl-sql, chat, profile, plot)

🟦 1) SEMANTIC SEARCH — /search (10 test cases)
✔ Basic search
{ "query": "temperature profile", "top_k": 5 }

✔ Salinity search
{ "query": "high salinity surface water", "top_k": 5 }

✔ Oxygen / BGC term
{ "query": "oxygen levels in the deep ocean", "top_k": 5 }

✔ Strange query (model robustness)
{ "query": "weird query 12345", "top_k": 5 }

✔ Long text query
{ "query": "Explain heatwaves under ocean circulation and ENSO", "top_k": 5 }

✔ Null-like query
{ "query": "", "top_k": 5 }

✔ Extreme top_k
{ "query": "antarctic cold water", "top_k": 20 }

✔ Mixed oceanographic terms
{ "query": "warm core eddies", "top_k": 5 }

✔ Deep ocean query
{ "query": "below 2000m temperature", "top_k": 5 }

✔ Salinity + depth
{ "query": "salinity at 500m depth", "top_k": 5 }

🟩 2) GEO SEARCH — /geo_search (10 test cases)
✔ Valid normal case
{
  "lat": -30,
  "lon": 150,
  "radius_km": 200,
  "query": "warm water",
  "top_k": 5
}

✔ Only lat/lon (semantic optional)
{
  "lat": -40,
  "lon": -10,
  "radius_km": 100,
  "query": null,
  "top_k": 5
}

✔ Edge radius (large)
{
  "lat": 20,
  "lon": 70,
  "radius_km": 1000,
  "query": "arabian sea",
  "top_k": 10
}

✔ Arctic region
{
  "lat": 70,
  "lon": -150,
  "radius_km": 200,
  "query": "cold water",
  "top_k": 5
}

✔ Southern Ocean region
{
  "lat": -60,
  "lon": 40,
  "radius_km": 300,
  "query": "deep water",
  "top_k": 5
}

✔ Radius = 0
{
  "lat": 30,
  "lon": 60,
  "radius_km": 0,
  "query": "surface",
  "top_k": 5
}

✔ Very large radius (stress test)
{
  "lat": 0,
  "lon": 0,
  "radius_km": 3000,
  "query": "",
  "top_k": 5
}

✔ Invalid coords: lon > 180°
{
  "lat": 10,
  "lon": 200,
  "radius_km": 100,
  "query": "test",
  "top_k": 5
}

✔ Invalid coords: lat too high
{
  "lat": 200,
  "lon": 50,
  "radius_km": 200,
  "query": "",
  "top_k": 5
}

✔ Boundary test: lon = -180
{
  "lat": 5,
  "lon": -180,
  "radius_km": 100,
  "query": "test",
  "top_k": 5
}

🟨 3) NL → SQL — /nl_query (10 powerful test cases)
✔ Basic
{
  "question": "show me temperature near latitude -35"
}

✔ Use floats table
{
  "question": "list all float positions near 10 latitude and 60 longitude"
}

✔ Combined join query
{
  "question": "give me temperature and salinity for float 1902043 at cycle 252"
}

✔ Depth-based
{
  "question": "what is the temperature at 1000 meters depth"
}

✔ Time filtering
{
  "question": "show measurements after 2020"
}

✔ Wide area LAT + LON
{
  "question": "show temperature between lat -20 and 20"
}

✔ Invalid query
{
  "question": "delete all tables"
}

✔ Very vague NL
{
  "question": "cold places in ocean"
}

✔ Multi-column
{
  "question": "show depth temperature and salinity for all floats"
}

✔ Specific float_id
{
  "question": "show profile data for float 2900345"
}

🟪 4) Chat — /chat (5 test cases)
Basic:
{
  "query": "Explain thermocline",
  "top_k": 3
}

Hard question:
{
  "query": "How does ENSO affect sea surface temperature?",
  "top_k": 5
}

Data-based:
{
  "query": "What do profiles near -40 latitude show?",
  "top_k": 3
}

Long query:
{
  "query": "Summarize deep water formation and its temperature gradient",
  "top_k": 3
}

Empty:
{
  "query": "",
  "top_k": 3
}

🟥 5) Profile API — GET /profile/{float_id}/{cycle}
Valid:
GET /profile/1902043/252

Invalid float:
GET /profile/9999/1

Out of range cycle:
GET /profile/1902043/999

🟫 6) Plot API — GET /plot/profile/{float_id}/{cycle}
Valid:
GET /plot/profile/1902043/252?plot_type=temp

Salinity:
GET /plot/profile/1902043/252?plot_type=sal

Dual plot:
GET /plot/profile/1902043/252?plot_type=both

Invalid float:
GET /plot/profile/11111/1?plot_type=temp

🎁 Bonus: Want pytest automated tests for all APIs?

I can generate a complete automated pytest suite like this:

tests/
  ├── test_semantic.py
  ├── test_geo.py
  ├── test_nl_query.py
  ├── test_chat.py
  ├── test_profile.py
  └── test_plot.py

  # cd C:\oceanBackend\OceanIQB
# venv\Scripts\activate
# uvicorn main:app --reload

#def test_env_config():
    print("=== .env File Testing ===")
    print(f"Database URL: {DATABASE_URL}")
    # print(f"FAISS Folder: {FAISS_DIR}")
    # print(f"Model Name: {EMBED_MODEL_NAME}")
    
    # Database connection test karo
    # try:
    #     with engine.connect() as conn:
    #         result = conn.execute(text("SELECT 1"))
    #     print("✅ Database connection successful!")
    # except Exception as e:
    #     print(f"❌ Connection failed: {e}")

        test_env_config()
