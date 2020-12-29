import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from DBConnector import DBConnector, WriteConnector
from functions import Logger
from optparse import OptionParser

logger = Logger()


class FTX(object):
    def __init__(self, name):
        self.connection = WriteConnector(name=name).get_connector()
        self.url = 'https://ftx.com/api'
        self.session = requests.session()

    def get_instruments(self):
        symbols = self.session.get(self.url + "/markets").json()['result']
        return [symbol['name'] for symbol in symbols if symbol['baseCurrency'] is not None]

    def fetch_kline(self, symbol, start_ts: int, end_ts: int, freq=60):
        data = self._get_kline_from_api(symbol=symbol, start=start_ts, end=end_ts, freq=freq)
        # print(data)
        data['start_datetime'] = data['startTime'].apply(lambda x: int(datetime.fromisoformat(x).timestamp()))
        del data['startTime']
        del data['time']
        data['freq_seconds'] = freq
        data['global_symbol'] = "SPOT-{}".format(symbol)
        data.to_sql(name='ftx_spot_official_klines', con=self.connection, if_exists='append',
                    method='multi', index=False)

    def _get_kline_from_api(self, symbol, start: int, end: int, freq):
        klines = []
        while start < end:
            this_url = self.url + f"/markets/{symbol}/candles?resolution={freq}&limit=2000" \
                                  f"&start_time={start}&end_time={min(start + freq * 1999, end)}"
            data = self.session.get(this_url).json()
            klines.extend(data['result'])
            start += 2000 * freq
            time.sleep(0.01)
        klines = pd.DataFrame(klines)
        return klines


def pull_spot_klines(name, freq=60):
    ftx_mds = FTX(name=name)
    symbols = ftx_mds.get_instruments()
    sql = "SELECT max(start_datetime) from ftx_spot_official_klines where global_symbol="
    db_helper = DBConnector(name=name)
    _end_ts = int(datetime.now().timestamp())
    _end_ts = (_end_ts // 60 - 1) * 60
    for _symbol in symbols:
        this_sql = sql + f"'SPOT-{_symbol}'"
        _start_ts = db_helper.run_query(sql=this_sql)
        if _start_ts[0][0] is None:
            _start_ts = int(datetime(2019, 1, 1).timestamp())
        else:
            _start_ts = _start_ts[0][0] + freq
        if _start_ts < _end_ts:
            try:
                logger.Info(f"Start pulling klines: FTX SPOT: {_symbol} from {_start_ts} to {_end_ts}")
                ftx_mds.fetch_kline(symbol=_symbol, start_ts=_start_ts, end_ts=_end_ts)
            except Exception as ex:
                logger.Critical(str(ex))
                logger.Error(f"Failed to pull kline for FTX SPOT: {_symbol}")
        else:
            logger.Info(f"Currently no updates for FTX SPOT: {_symbol}")


if __name__ == "__main__":
    parse = OptionParser()
    parse.add_option('--spot', dest='is_spot', action='store_true')
    parse.add_option('-f', dest='is_prod', action='store_true')
    (optional_params, args) = parse.parse_args()
    if optional_params.is_spot:
        if optional_params.is_prod:
            pull_spot_klines(name='crypto')
        else:
            pull_spot_klines(name='public')

    else:
        if optional_params.is_prod:
            pull_spot_klines(name='crypto')
        else:
            pull_spot_klines(name='public')
