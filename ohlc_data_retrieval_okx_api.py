# Получение данных по Api для определенного ТФ

from okx import MarketData 
import pandas as pd
import time
import httpx

flag = "0"  # live trading: 0, demo trading: 1

trading_pair = "BTC-USDT-SWAP"  
bar = "1Dutc" 
limit = 100
after = str(int(time.time() * 1000))

marketDataAPI = MarketData.MarketAPI(flag=flag, debug=False)

ddr = pd.DataFrame(columns=["Date","Open","High","Low","Close"])

while True:
  try:
    # Получение данных
    response = pd.DataFrame(marketDataAPI.get_history_candlesticks(
      instId=trading_pair,
      bar=bar,
      after=after,
      limit=limit
    ))

    # Проверка на пустой ответ
    if response.empty:
      break

    # Преобразование данных и добавление в ddr
    Df = response['data']
    data_list = Df.tolist()
    res = pd.DataFrame(data_list, columns=["Date","Open","High","Low","Close","Vol","volCcy","volCcyQuote","confirm"])
    res = res.iloc[:, :5]

    # Обновление параметра before для следующего запроса
    after = str(res.iloc[res.shape[0] - 1, 0])  

    # Явное приведение строк к числовому типу перед преобразованием
    res['Date'] = pd.to_numeric(res['Date'])
    res['Date'] = pd.to_datetime(res['Date'], unit='ms')

    ddr = pd.concat([ddr, res], ignore_index=True)
  
  except httpx.TimeoutException:
    print("Error fetching market data: The read operation timed out")
  except httpx.ConnectError:
    print("Error fetching market data: Connection error")
  except httpx.RequestError as e:
    print(f"Error fetching market data: {e}")
  except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Сбор данных завершен.")
print(ddr)

ddr.to_csv('data_OHLC_3m_BTC-USDT-SWAP_OKX.csv', index=False)
print("DataFrame успешно сохранен в файл data.csv")
