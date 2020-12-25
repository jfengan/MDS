import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from DBConnector import DBConnector, WriteConnector
from functions import Logger

logger = Logger()


class Binance_Spot_Rest(object):
    def __init__(self):
        self.db_connection = DBConnector(name='public')
        self.connector = WriteConnector(name="public").get_connector()
        self.session = requests.session()
        self.url = "https://api.binance.com"
        self.__freq_mapping = {
            '60': '1m',
            '180': '3m',
            '300': '5m',
            '900': '15m',
            '1800': '30m',
            '3600': '1h',
            '14400': '4h',
            '86400': '1d'
        }

    def __del__(self):
        self.session.close()
        # self.connector.close()

    def get_instruments(self):
        data = self.session.get(self.url + '/api/v3/exchangeInfo').json()
        return [[item['symbol'], item['baseAsset'], item['quoteAsset']] for item in data['symbols']]

    def __get_kline_by_instrument(self, instrument_name, start_datetime, end_datetime, freq, base_asset, quote_asset):
        data = self.session.get(self.url + "/api/v3/klines?symbol={}&interval={}&limit=1500&startTime={}&endTime={}".
                                format(instrument_name, self.__freq_mapping[str(freq)],
                                       1000 * start_datetime,
                                       1000 * end_datetime)).json()
        data = pd.DataFrame(data, columns=['Open time', 'open', 'high', 'low', "close", 'volume', 'close time',
                                           'amount', 'trades', 'taker base', 'taker quote', 'ignore'])
        if len(data.index) > 0:
            data['start_datetime'] = data['Open time'].apply(lambda x: x // 1000)
            data = data[['open', 'high', 'low', 'close', 'volume', 'amount', 'start_datetime']]
            data['freq_seconds'] = freq
            data['global_symbol'] = 'SPOT-{}/{}'.format(base_asset, quote_asset)
            return data
        else:
            return None

    def get_klines(self, symbol: list, freq, start_ts: int, end_ts: int):
        while start_ts < end_ts:
            data = self.__get_kline_by_instrument(instrument_name=symbol[0], start_datetime=start_ts,
                                                  end_datetime=start_ts + 60 * 999, freq=freq, base_asset=symbol[1],
                                                  quote_asset=symbol[2])
            if data is not None:
                data = data[data['start_datetime'] < end_ts]
                data.to_sql(name='binance_spot_official_klines', index=False, if_exists='append',
                            con=self.connector, method='multi')
                time.sleep(0.1)
            start_ts += 60 * 1000



if __name__ == "__main__":
    _end_str = datetime.now().strftime("%Y-%m-%d")
    _end_ts = (datetime.strptime(_end_str, "%Y-%m-%d") - timedelta(days=1)).timestamp()
    bn_kline = Binance_Spot_Rest()
    symbols = bn_kline.get_instruments()
    base_sql = "SELECT max(start_datetime) from binance_spot_official_klines where global_symbol="
    for _symbol in symbols:
        sql = base_sql + f"'SPOT-{_symbol[1]}/{_symbol[2]}'"
        _start_ts = bn_kline.db_connection.run_query(sql=sql)
        if _start_ts[0][0] is None:
            _start_ts = int(datetime(2019, 1, 1).timestamp())
        else:
            _start_ts = _start_ts[0][0] + 60
        if _start_ts >= _end_ts:
            logger.Info(f"No updates for binance spot: {_symbol[0]}")
        else:
            logger.Info(f"Start pulling kline of binance spot {_symbol[0]} from {_start_ts} to {_end_ts}")
            bn_kline.get_klines(symbol=_symbol, freq=60, start_ts=_start_ts, end_ts=int(_end_ts))
