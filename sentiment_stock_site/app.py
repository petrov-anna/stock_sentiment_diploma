from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from services.forecast import forecast_func
import pandas as pd
import time
from datetime import datetime
import warnings
import uvicorn
warnings.filterwarnings('ignore')


# Инициализация с CORS
app = FastAPI(title="Stock Sentiment Forecast")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Монтирование статических файлов
app.mount("/static", StaticFiles(directory="static"), name="static")


# Загрузка данных
DATA_PATH = "data/stock_data.csv"
try:
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])
    print(f"Data loaded: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
except Exception as e:
    print(f"Error loading data: {e}")
    df = pd.DataFrame()


# HTML страницы
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()

@app.get("/forecast", response_class=HTMLResponse)
async def forecast_page():
    with open("templates/forecast.html", encoding="utf-8") as f:
        return f.read()

@app.get("/analysis", response_class=HTMLResponse)
async def analysis_page():
    with open("templates/analysis.html", encoding="utf-8") as f:
        return f.read()


# API: исторические данные
@app.get("/history")
async def get_history(days: int = Query(30, ge=1, le=365)):
    try:
        print(f"GET /history called with days={days}")
        
        if df.empty:
            print("No data available")
            return JSONResponse(
                content={"error": "No data available"},
                status_code=404
            )
        
        data = df.sort_values("date").tail(days)
        print(f"Returning {len(data)} rows")
        
        response = {
            "dates": data["date"].dt.strftime("%Y-%m-%d").tolist(),
            "close": data["close"].tolist(),
            "sentiment": data.get("sentiment_score", [0]*len(data)).tolist()
        }
        
        return JSONResponse(content=response)
        
    except Exception as e:
        print(f"Error in /history: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


# API: прогноз
@app.get("/api/forecast")
async def forecast_api(
    n_days: int = Query(5, ge=1, le=30),
    sentiment: int = Query(0, ge=-1, le=1)
):
    try:
        start_time = time.time()
        print(f"GET /api/forecast called: n_days={n_days}, sentiment={sentiment}")
        
        if df.empty:
            return JSONResponse(
                content={"error": "Данные не загружены"},
                status_code=500
            )
        
        # Получаем исторические данные
        data = df.sort_values("date")
        history = data.tail(30).copy()
        
        if len(history) < 5:
            return JSONResponse(
                content={"error": "Недостаточно исторических данных"},
                status_code=400
            )
        
        print(f"History shape: {history.shape}")
        print(f"Last 5 closes: {history['close'].tail(5).values.tolist()}")
        
        forecast_values = forecast_func(history, n_days, sentiment)
        
        processing_time = time.time() - start_time
        print(f"Forecast generated in {processing_time:.2f}s: {forecast_values}")
        
        response = {
            "sentiment": sentiment,
            "forecast": forecast_values,
            "model": "simplified",
            "processing_time": f"{processing_time:.2f}s",
            "last_price": float(history['close'].iloc[-1]) if not history.empty else None
        }
        
        return JSONResponse(content=response)
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Forecast API error: {error_details}")
        
        return JSONResponse(
            content={
                "error": "Внутренняя ошибка сервера",
                "details": str(e)
            },
            status_code=500
        )


# Отладочные эндпоинты
@app.get("/debug/data")
async def debug_data():
    """Проверка данных"""
    if df.empty:
        return JSONResponse(content={"status": "no_data"})
    
    return JSONResponse(content={
        "rows": len(df),
        "columns": list(df.columns),
        "date_range": {
            "min": df["date"].min().strftime("%Y-%m-%d"),
            "max": df["date"].max().strftime("%Y-%m-%d")
        },
        "sample": df.head(3).to_dict(orient="records")
    })

@app.get("/debug/cors-test")
async def cors_test():
    """Тестовый эндпоинт для проверки CORS"""
    return JSONResponse(content={
        "message": "CORS test successful",
        "timestamp": datetime.now().isoformat(),
        "allowed_origins": ["*"]
    })

@app.get("/health")
async def health():
    """Проверка здоровья сайта"""
    try:
        status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "data_loaded": not df.empty,
            "data_rows": len(df) if not df.empty else 0,
            "cors_enabled": True
        }
        
        return JSONResponse(content=status)
        
    except Exception as e:
        return JSONResponse(
            content={"status": "unhealthy", "error": str(e)},
            status_code=500
        )


# Запуск с логированием CORS
if __name__ == "__main__":
    print("Starting server with CORS enabled...")
    print("Access the app at:")
    print("  - http://localhost:8000")
    print("  - http://127.0.0.1:8000")
    print("  - http://localhost:8000/forecast")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="debug",
        access_log=True
    )