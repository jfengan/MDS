import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from DBConnector import DBConnector, WriteConnector
from functions import Logger


logger = Logger()


class CoinBase(object):
    def __init__(self, name):
        self.name = name
        self.connection = WriteConnector(name=name).get_connector()
        self.session = requests.session()
        self.url = 'https://api.pro.coinbase.com'

    def get_insturments(self):
        coins = self.session.get(f'{self.url}/products').json()
        return [coin['id'] for coin in coins]

    def fetch_kline(self, symbol: str, start_ts: int, end_ts: int, freq=60):
        data = self._get_hist_klines(symbol=symbol, start_ts=start_ts, end_ts=end_ts, freq=freq)
        data = pd.DataFrame(data, columns=['start_datetime', 'open', 'high', 'low', 'close', 'volume'])
        data['freq_seconds'] = freq
        data['global_symbol'] = 'SPOT-{}'.format(symbol.replace('-', '/'))
        data.to_sql(name='coinbase_spot_official_klines', index=False, con=self.connection,
                    method='multi', if_exists='append')

    def _get_hist_klines(self, symbol: str, start_ts: int, end_ts: int, freq: int):
        klines = []
        while start_ts < end_ts:
            start = datetime.fromtimestamp(start_ts).replace(microsecond=0).isoformat()
            end = datetime.fromtimestamp(start_ts + 299 * freq).replace(microsecond=0).isoformat()
            this_url = self.url + f"/products/{symbol}" \
                       + "/candles?start={}&end={}&granularity={}".format(start, end, freq)
            data = self.session.get(this_url)
            klines.extend(data.json())
            start_ts += 300 * freq
            time.sleep(0.2)
        return klines


def pull_coinbase_kline():
    coinbase = CoinBase(name='public')
    coins = coinbase.get_insturments()
    db_helper = DBConnector(name='public')
    _end_str = datetime.now().strftime("%Y-%m-%d")
    _end_ts = int((datetime.strptime(_end_str, "%Y-%m-%d") - timedelta(days=1)).timestamp())
    sql = "SELECT max(start_datetime) from coinbase_spot_official_klines where global_symbol="
    for coin in coins:
        _this_sql = sql + "'SPOT-{}'".format(coin.replace('-', '/'))
        _start_ts = db_helper.run_query(sql=_this_sql)
        if _start_ts[0][0] is None:
            _start_ts = int(datetime(2019, 1, 1).timestamp())
        else:
            _start_ts = _start_ts[0][0] + 60
        if _start_ts < _end_ts:
            try:
                logger.Info(f"Start pulling klines: COINBASE SPOT: {coin}")
                coinbase.fetch_kline(symbol=coin,
                                     start_ts=_start_ts,
                                     end_ts=_end_ts)
            except Exception as ex:
                logger.Critical(str(ex))
                logger.Error(f"Failed to pull kline: COINBASE SPOT: {coin}")
        else:
            logger.Info(f"No update for COINBASE SPOT: {coin}")


if __name__ == "__main__":
    pull_coinbase_kline()