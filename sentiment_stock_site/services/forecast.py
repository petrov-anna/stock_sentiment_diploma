import numpy as np


def forecast_func(history_df, n_days=5, sentiment=0):
    try:
        if history_df.empty or 'close' not in history_df.columns:
            return [100.0] * n_days
        
        prices = history_df['close'].tail(20).values
        
        if len(prices) < 2:
            last_price = prices[-1] if len(prices) > 0 else 100
            return [float(last_price)] * n_days
        
        # Расчет простого тренда
        if len(prices) >= 5:
            trend = (prices[-1] - prices[-5]) / 5
        elif len(prices) >= 2:
            trend = (prices[-1] - prices[0]) / len(prices)
        else:
            trend = 0
        
        last_price = float(prices[-1])
        
        # Влияние sentiment (1-2%)
        sentiment_multiplier = 1.0 + (sentiment * 0.015)
        
        # Генерация прогноза
        forecasts = []
        for i in range(1, n_days + 1):
            # Линейный тренд + sentiment
            forecast_price = last_price + (trend * i * sentiment_multiplier)
            
            # Добавляем небольшой случайный шум (±0.5%)
            noise = np.random.uniform(-0.005, 0.005)
            forecast_price = forecast_price * (1 + noise)
            
            # Не даем уйти ниже 10% от исходной цены
            forecast_price = max(forecast_price, last_price * 0.1)
            forecasts.append(round(forecast_price, 2))
        
        return forecasts
        
    except Exception as e:
        print(f"Simple forecast error: {e}")
        return [100.0] * n_days