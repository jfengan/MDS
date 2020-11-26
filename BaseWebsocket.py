import aiohttp
import inspect


class WSBaseServer(object):
    def __init__(self,
                 url,
                 logger,
                 callback_when_data,
                 callback_when_error,
                 callback_when_close,
                 callback_after_connection,
                 heartbeat=5,
                 ssl=True):

        assert callable(callback_when_data) or inspect.iscoroutinefunction(callback_when_data)
        assert callable(callback_when_close) or inspect.iscoroutinefunction(callback_when_close)
        assert callable(callback_when_error) or inspect.iscoroutinefunction(callback_when_error)
        assert callable(callback_after_connection) or inspect.iscoroutinefunction(callback_after_connection)

        self.ws = None
        self.url = url
        self.logger = logger
        self.heartbeat = heartbeat
        self.ssl = ssl

        self.callback_when_data = callback_when_data
        self.callback_when_close = callback_when_close
        self.callback_when_error = callback_when_error
        self.callback_after_connection = callback_after_connection

    async def connect_to_server(self):
        session = aiohttp.ClientSession()
        self.logger.info("Connecting to {}".format(self.url))
        self.ws = await session.ws_connect(url=self.url, ssl=self.ssl, heartbeat=self.heartbeat)
        self.logger.info("Connection established")
        if inspect.iscoroutinefunction(self.callback_after_connection):
            await self.callback_after_connection()
        else:
            self.callback_after_connection()

    async def send_message(self, message: list):
        for msg in message:
            await self.ws.send_json(msg)

    async def receive_data(self):
        while True:
            msg = await self.ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT or msg.type == aiohttp.WSMsgType.BINARY:
                self.callback_when_data(msg)
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                self.logger.info("Closed: {}".format(msg))
                #await self.callback_when_close()
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.logger.info("Error: {}".format(msg))
                #await self.callback_when_error()
            else:
                self.logger.warning("receive msg {} with type {}. Not recognized. Ignore"
                                    .format(msg.data, msg.type))
