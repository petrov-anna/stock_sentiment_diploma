import numpy as np

def compute_indicators(df):
    df = df.copy()

    df['MA7'] = df['close'].rolling(7).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['20SD'] = df['close'].rolling(20).std()

    df['upper_band'] = df['MA20'] + 2 * df['20SD']
    df['lower_band'] = df['MA20'] - 2 * df['20SD']

    df['EMA'] = df['close'].ewm(span=20, adjust=False).mean()
    df['MACD'] = (
        df['close'].ewm(span=12).mean()
        - df['close'].ewm(span=26).mean()
    )

    df['logmomentum'] = np.log(df['close'] / df['close'].shift(1))

    return df