import yfinance as yf
import os

symbol = "EURUSD=X"
period = "2d"
interval = "1m"
data = yf.download(symbol, period=period, interval=interval)

print(data)

# Crear las carpetas si no existen
os.makedirs('data/yahoo', exist_ok=True)

# Guardar el archivo en formato CSV
data.to_csv('data/yahoo.csv')
