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

    def decode(self, data):
        return data

    async def subscribe(self):
        channels = []
        for symbol in self.symbols[0:2]:
            channel = ["trades.{}.raw".format(symbol), "book.{}.raw".format(symbol)]
            if symbol in ['BTC-PERPETUAL', 'ETH-PERPETUAL']:
                channel.append("perpetual.{}.raw".format(symbol))
            channel.append("quote.{}".format(symbol))
            channel.append("ticker.{}.raw".format(symbol))
            message = {"jsonrpc": "2.0",
                       "method": "public/subscribe",
                       "id": 42,
                       "params": {"channels": channel}
                       }
            channels.append(message)
        await self.ws.send_message(message=channels)

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
