from BaseWebsocketHandler import WSHandler
from deribit.deribit_rest import get_symbols
from logging.handlers import TimedRotatingFileHandler
import asyncio
import logging


class DeribitRecorder(WSHandler):
    def __init__(self, url, symbols):
        logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log = TimedRotatingFileHandler("Deribit_Tick.log", 'H', 1, 0)
        # log = TimedRotatingFileHandler(filename='Deribit_Tick.log', when='midnight', backupCount=30)
        log.setLevel(logging.INFO)
        log.setFormatter(logformatter)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log)
        super().__init__(url, logger=self.logger)
        self.symbols = symbols
        self.index_names = ['btc_usd', 'eth_usd']

    @staticmethod
    def get_index_channel(index: list):
        index_channels = []
        for index_name in index:
            index_channels.append("deribit_price_index.{}".format(index_name))
            index_channels.append("deribit_price_ranking.{}".format(index_name))
            index_channels.append("estimated_expiration_price.{}".format(index_name))
            index_channels.append("markprice.options.{}".format(index_name))
        return index_channels

    @staticmethod
    def get_symbol_channel(symbols: list):
        channels = []
        # for symbol in symbols:
        for symbol in ['BTC-PERPETUAL']:
            channels.append("trades.{}.raw".format(symbol))
            # channels.append("book.{}.raw".format(symbol))
            if symbol in ['BTC-PERPETUAL', 'ETH-PERPETUAL']:
                channels.append("perpetual.{}.raw".format(symbol))
            # channels.append("quote.{}".format(symbol))
            channels.append("ticker.{}.raw".format(symbol))
        return channels

    def decode(self, data):
        return data

    async def subscribe(self):
        # subscribe index price
        """index_channels = self.get_index_channel(index=self.index_names)
        message = {"jsonrpc": "2.0",
                   "method": "public/subscribe",
                   "id": 42,
                   "params": {"channels": index_channels}
                   }
        await self.ws.send_message([message])"""

        # subscribe tickers of symbols
        symbol_channels = self.get_symbol_channel(symbols=self.symbols)
        message = {"jsonrpc": "2.0",
                   "method": "public/subscribe",
                   "id": 42,
                   "params": {"channels": symbol_channels}
                   }
        await self.ws.send_message([message])

    async def heartbeat_service(self):
        while True:
            await self.ws.send_message([
                {
                    "jsonrpc": "2.0",
                    "id": 9929,
                    "method": "pong",
                }
            ])
            await asyncio.sleep(60)


if __name__ == "__main__":
    _symbols = get_symbols()

    async def t(_loop):
        recorder = DeribitRecorder(url="wss://www.deribit.com/ws/api/v2",
                                   symbols=["BTC-PERPETUAL"])
        await recorder.connect(_loop=_loop)
        asyncio.ensure_future(recorder.process_data(), loop=_loop)
        await recorder.heartbeat_service()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(t(loop))
