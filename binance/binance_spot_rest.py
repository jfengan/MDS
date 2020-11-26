import requests


def get_symbols():
    data = requests.get(
        url="https://api.binance.com/api/v3/exchangeInfo"
    ).json()
    data = [[item['symbol'], item['baseAsset'], item['quoteAsset'] ] for item in data['symbols']]
    print(data)


if __name__ == "__main__":
    get_symbols()
