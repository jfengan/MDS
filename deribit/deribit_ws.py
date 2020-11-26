from BaseWebsocketHandler import WSHandler
from deribit.deribit_rest import get_symbols
from logging.handlers import TimedRotatingFileHandler
import asyncio
import logging


class DeribitRecorder(WSHandler):
    def __init__(self, url, symbols):
        logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log = TimedRotatingFileHandler("Deribit_Tick.log", 'M', 1, 0)
        # log = TimedRotatingFileHandler(filename='Deribit_Tick.log', when='midnight', backupCount=30)
        log.setLevel(logging.INFO)
        log.setFormatter(logformatter)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log)
        super().__init__(url, logger=self.logger)
        self.symbols = symbols
        self.index_names = ['btc_usd', 'eth_usd']

    def decode(self, data):
        return data

    async def subscribe(self):
        # subscribe index price
        index_channels = []
        for index_name in self.index_names:
            self.logger.info('subscribing {} index price and etc.'.format(index_name))
            index_channels.append("deribit_price_index.{}".format(index_name))
            index_channels.append("deribit_price_ranking.{}".format(index_name))
            index_channels.append("estimated_expiration_price.{}".format(index_name))
            index_channels.append("markprice.options.{}".format(index_name))
        message = {"jsonrpc": "2.0",
                   "method": "public/subscribe",
                   "id": 42,
                   "params": {"channels": index_channels}
                   }
        await self.ws.send_message([message])

        # subscribe tickers of symbols
        channels = []
        for symbol in self.symbols:
            channels.append("trades.{}.raw".format(symbol))
            channels.append("book.{}.raw".format(symbol))
            if symbol in ['BTC-PERPETUAL', 'ETH-PERPETUAL']:
                channels.append("perpetual.{}.raw".format(symbol))
            channels.append("quote.{}".format(symbol))
            channels.append("ticker.{}.raw".format(symbol))
        message = {"jsonrpc": "2.0",
                   "method": "public/subscribe",
                   "id": 42,
                   "params": {"channels": channels}
                   }
        await self.ws.send_message([message])

    async def heartbeat_service(self):
        while True:
            await self.ws.send_message([
                {
                    "jsonrpc": "2.0",
                    "id": 9929,
                    "method": "ping",
                }
            ])
            await asyncio.sleep(3)


if __name__ == "__main__":
    _symbols = get_symbols()

    async def t(_loop):
        recorder = DeribitRecorder(url="wss://www.deribit.com/ws/api/v2",
                                   symbols=_symbols)
        await recorder.connect(_loop=_loop)
        asyncio.ensure_future(recorder.process_data(), loop=_loop)
        await recorder.heartbeat_service()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(t(loop))
