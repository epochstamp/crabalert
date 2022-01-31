import discord
from typing import Optional, Any, Dict, List, Tuple, Callable
import asyncio
import aiohttp
import discord.http
from discord.voice_client import VoiceClient
from discord.state import ConnectionState
from discord.http import HTTPClient
from discord.http import Route, HTTPException, LoginFailure, DiscordClientWebSocketResponse
import logging

_log = logging.getLogger(__name__)

class CustomHTTP(discord.http.HTTPClient):
    def recreate(self):
        if self.__session.closed:
            self.__session = aiohttp.ClientSession(connector=self.connector, ws_response_class=DiscordClientWebSocketResponse, trust_env=True)

    async def static_login(self, token, bot):
        # Necessary to get aiohttp to stop complaining about session creation
        self.__session = aiohttp.ClientSession(connector=self.connector, ws_response_class=DiscordClientWebSocketResponse)
        old_token, old_bot = self.token, self.bot_token
        self._token(token, bot=bot)
        print(self)
        try:
            data = await self.request(Route('GET', '/users/@me'))
        except HTTPException as exc:
            self._token(old_token, bot=old_bot)
            if exc.response.status == 401:
                raise LoginFailure('Improper token has been passed.') from exc
            raise

        return data

class CustomClient(discord.Client):

    def __init__(
        self,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **options: Any,
    ):
        # self.ws is set in the connect method
        self.ws: DiscordWebSocket = None  # type: ignore
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop() if loop is None else loop
        self._listeners: Dict[str, List[Tuple[asyncio.Future, Callable[..., bool]]]] = {}
        self.shard_id: Optional[int] = options.get('shard_id')
        self.shard_count: Optional[int] = options.get('shard_count')

        connector: Optional[aiohttp.BaseConnector] = options.pop('connector', None)
        proxy: Optional[str] = options.pop('proxy', None)
        proxy_auth: Optional[aiohttp.BasicAuth] = options.pop('proxy_auth', None)
        unsync_clock: bool = options.pop('assume_unsync_clock', True)
        self.http: HTTPClient = CustomHTTP(connector, proxy=proxy, proxy_auth=proxy_auth, unsync_clock=unsync_clock, loop=self.loop)

        self._handlers: Dict[str, Callable] = {
            'ready': self._handle_ready
        }

        self._hooks: Dict[str, Callable] = {
            'before_identify': self._call_before_identify_hook
        }

        self._enable_debug_events: bool = options.pop('enable_debug_events', False)
        self._connection: ConnectionState = self._get_state(**options)
        self._connection.shard_count = self.shard_count
        self._closed: bool = False
        self._ready: asyncio.Event = asyncio.Event()
        self._connection._get_websocket = self._get_websocket
        self._connection._get_client = lambda: self

class CustomBot(discord.ext.commands.bot.BotBase, CustomClient):
    pass