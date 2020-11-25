from abc import abstractmethod
from BaseWebsocket import WSBaseServer
from datetime import datetime
import aiohttp
import asyncio


class WSHandler(object):
    def __init__(self, url, logger):
        self.ws = WSBaseServer(url=url,
                               logger=logger,
                               callback_when_data=self.on_data,
                               callback_when_error=self.on_error,
                               callback_when_close=self.on_close,
                               callback_after_connection=self.on_open)

        self.data_queue = asyncio.Queue(maxsize=100000)
        self.logger = logger

    async def connect(self, _loop):
        await self.ws.connect_to_server()
        asyncio.ensure_future(self.ws.receive_data(), loop=_loop)

    def on_data(self, data):
        try:
            if data.type == aiohttp.WSMsgType.TEXT:
                self.data_queue.put_nowait(data.data)
            elif data.type == aiohttp.WSMsgType.BINARY:
                data = self.decode(data.data)
                self.data_queue.put_nowait(data)
        except asyncio.QueueFull:
            self.logger.warning("Queue Full. Drop data {}".format(data))

    async def process_data(self):
        while True:
            data = await self.data_queue.get()
            self.logger.info((int(datetime.now().timestamp() * 1000), data))

    async def on_error(self):
        await self.ws.ws.close()
        self.ws.ws = None
        await asyncio.sleep(5)
        await self.ws.connect_to_server()
        # asyncio.ensure_future(self.ws.receive_data(), loop=_loop)

    async def on_close(self):
        self.ws.ws = None
        await asyncio.sleep(5)
        await self.ws.connect_to_server()

    async def on_open(self):
        await self.subscribe()

    @abstractmethod
    def decode(self, data):
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self):
        raise NotImplementedError
