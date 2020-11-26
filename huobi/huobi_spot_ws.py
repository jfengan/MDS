from BaseWebsocketHandler import WSHandler
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import asyncio
import gzip
import json
import logging


class HuobiRecorder(WSHandler):
    def __init__(self, url:str, symbols: list):
        logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log = TimedRotatingFileHandler("Huobi_Spot_Tick.log", 'H', 1, 0)
        log.setLevel(logging.INFO)
        log.setFormatter(logformatter)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log)

        super().__init__(url, logger=self.logger)
        self.symbols = symbols

    async def process_data(self):
        while True:
            data = await self.data_queue.get()
            if 'ping' in data:
                await self.ws.send_message([{"pong": data['ping']}])
            self.logger.info((int(datetime.now().timestamp() * 1000), data))

    def decode(self, data):
        data = json.loads(gzip.decompress(data).decode())
        return data

    async def heartbeat_service(self):
        while True:
            await asyncio.sleep(60)

    async def subscribe(self):
        for symbol in self.symbols:
            await self.ws.send_message(message=[
                {
                    "sub": "market.{}.bbo".format(symbol.lower()),
                    "id": "id"
                },

                {
                    "sub": "market.{}.trade.detail".format(symbol.lower()),
                    "id": "id"
                }
            ])


if __name__ == "__main__":
    async def t(_loop):
        recorder = HuobiRecorder(
            url="wss://api-aws.huobi.pro/ws",
            symbols=['BTCUSDT']
        )
        await recorder.connect(_loop=_loop)
        asyncio.ensure_future(recorder.process_data(), loop=_loop)
        await recorder.heartbeat_service()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(t(loop))
