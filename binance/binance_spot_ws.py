from BaseWebsocketHandler import WSHandler
from logging.handlers import TimedRotatingFileHandler
import asyncio
import logging


class BinanceRecorder(WSHandler):
    def __init__(self, url:str, symbols: list):
        logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log = TimedRotatingFileHandler("Binance_Tick.log", 'H', 1, 0)
        log.setLevel(logging.INFO)
        log.setFormatter(logformatter)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log)

        for symbol in symbols:
            # url += "/ws/{}@depth{}@100ms".format(symbol.lower(), 5)
            url += "/ws/{}@trade".format(symbol.lower())

        super().__init__(url, logger=self.logger)

    def decode(self, data):
        return data

    async def heartbeat_service(self):
        while True:
            # await self.ws.send_message([{"pong"}])
            await asyncio.sleep(60)

    async def subscribe(self):
        pass

if __name__ == "__main__":
    async def t(_loop):
        recorder = BinanceRecorder(url="wss://stream.binance.com:9443",
                                   symbols=["BTCUSDT"])
        await recorder.connect(_loop=_loop)
        asyncio.ensure_future(recorder.process_data(), loop=_loop)
        await recorder.heartbeat_service()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(t(loop))