import requests

def get_symbols():
    symbols = []
    for coin in ['BTC', 'ETH']:
        url = "https://www.deribit.com/api/v2/public/get_instruments?currency={}&expired=false".format(coin)
        data = requests.get(url).json()
        symbol = [item['instrument_name'] for item in data['result']]
        symbols.extend(symbol)
    return symbols