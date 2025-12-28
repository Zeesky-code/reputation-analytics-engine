from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import analytics
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        analytics.init_analytics()
    except Exception as e:
        print(f"Error initializing analytics: {e}")
    yield

app = FastAPI(lifespan=lifespan, title="Reputation Analytics Demo")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/business/{business_id}/overview")
async def get_overview(business_id: int):
    data = analytics.get_business_overview(business_id)
    if not data:
        raise HTTPException(status_code=404, detail="Business not found")
    return data

@app.get("/api/business/{business_id}/rating-trend")
async def get_rating_trend(business_id: int):
    return analytics.get_rating_trend_monthly(business_id)

@app.get("/api/business/{business_id}/sentiment-dist")
async def get_sentiment_dist(business_id: int):
    return analytics.get_sentiment_distribution(business_id)

@app.get("/api/business/{business_id}/deltas")
async def get_deltas(business_id: int):
    return analytics.get_performance_deltas(business_id)

@app.get("/api/business/{business_id}/benchmark")
async def get_benchmark(business_id: int):
    data = analytics.get_industry_benchmark(business_id)
    if not data:
        raise HTTPException(status_code=404, detail="Benchmark not found")
    return data

@app.get("/api/geo/overview")
async def get_geo_overview():
    return analytics.get_geo_sentiment_data()

@app.get("/api/geo/insight")
async def get_geo_insight():
    return {"insight": analytics.get_geo_insight()}

@app.get("/api/businesses")
async def list_businesses():
    # Helper to find a business to demo
    con = analytics.get_connection()
    res = con.execute("SELECT id, name, industry FROM raw_businesses LIMIT 10").fetchall()
    con.close()
    return [{"id": r[0], "name": r[1], "industry": r[2]} for r in res]

web_path = os.path.join(os.path.dirname(__file__), "../web")
if os.path.exists(web_path):
    app.mount("/", StaticFiles(directory=web_path, html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
