# coding=utf-8
import asyncio
import re
from typing import Any, Dict, Union

from aiohttp import ClientTimeout as Timeout
from deprecation import deprecated
from gotrue.types import AuthChangeEvent
from postgrest import AsyncPostgrestClient, AsyncRequestBuilder
from postgrest._async.request_builder import AsyncRPCFilterRequestBuilder
from postgrest.constants import DEFAULT_POSTGREST_CLIENT_TIMEOUT
from storage3.constants import DEFAULT_TIMEOUT as DEFAULT_STORAGE_CLIENT_TIMEOUT
from supafunc import AsyncFunctionsClient

from .lib.auth_client import SupabaseAuthClient
from .lib.client_options import ClientOptions
from .lib.storage_client import SupabaseStorageClient


class SupabaseException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class AsyncClient:
    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        options: ClientOptions = ClientOptions(),
    ):
        if not supabase_url:
            raise SupabaseException("supabase_url is required")
        if not supabase_key:
            raise SupabaseException("supabase_key is required")

        if not re.match(r"^(https?)://.+", supabase_url):
            raise SupabaseException("Invalid URL")

        if not re.match(
            r"^[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*$", supabase_key
        ):
            raise SupabaseException("Invalid API key")

        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        options.headers.update(self._get_auth_headers())
        self.options = options
        self.rest_url = f"{supabase_url}/rest/v1"
        self.realtime_url = f"{supabase_url}/realtime/v1".replace("http", "ws")
        self.auth_url = f"{supabase_url}/auth/v1"
        self.storage_url = f"{supabase_url}/storage/v1"
        self.functions_url = f"{supabase_url}/functions/v1"
        self.schema = options.schema

        self.auth = self._init_supabase_auth_client(
            auth_url=self.auth_url,
            client_options=options,
        )
        self.realtime = None
        self._postgrest = None
        self._storage = None
        self._functions = None
        self._init_task = asyncio.create_task(self._initialize())

    async def _initialize(self):
        await self.auth.on_auth_state_change(self._listen_to_auth_events)
        await self._init_storage()
        await self._init_postgrest()

    def __await__(self):
        yield from self._init_task.__await__()
        return self

    @deprecated("1.1.1", "1.3.0", details="Use `.functions` instead")
    def functions(self) -> AsyncFunctionsClient:
        return AsyncFunctionsClient(self.functions_url, self._get_auth_headers())

    async def table(self, table_name: str) -> AsyncRequestBuilder:
        return await self.from_(table_name)

    async def from_(self, table_name: str) -> AsyncRequestBuilder:
        postgrest_client = await self._init_postgrest()
        return postgrest_client.from_(table_name)

    async def rpc(self, fn: str, params: Dict[Any, Any]) -> AsyncRPCFilterRequestBuilder[Any]:
        postgrest_client = await self._init_postgrest()
        return await postgrest_client.rpc(fn, params)

    async def _init_postgrest(self):
        if self._postgrest is None:
            self.options.headers.update(await self._get_token_header())
            self._postgrest = self._init_postgrest_client(
                rest_url=self.rest_url,
                headers=self.options.headers,
                schema=self.options.schema,
                timeout=self.options.postgrest_client_timeout,
            )
        return self._postgrest

    async def _init_storage(self):
        if self._storage is None:
            headers = self._get_auth_headers()
            token_header = await self._get_token_header()
            headers.update(token_header)
            self._storage = self._init_storage_client(
                storage_url=self.storage_url,
                headers=headers,
                storage_client_timeout=self.options.storage_client_timeout,
            )
        return self._storage

    async def _init_functions(self):
        if self._functions is None:
            headers = self._get_auth_headers()
            token_header = await self._get_token_header()
            headers.update(token_header)
            self._functions = AsyncFunctionsClient(self.functions_url, headers)
        return self._functions

    @staticmethod
    def _init_storage_client(
        storage_url: str,
        headers: Dict[str, str],
        storage_client_timeout: int = DEFAULT_STORAGE_CLIENT_TIMEOUT,
    ) -> SupabaseStorageClient:
        return SupabaseStorageClient(storage_url, headers, storage_client_timeout)

    @staticmethod
    def _init_supabase_auth_client(
        auth_url: str,
        client_options: ClientOptions,
    ) -> SupabaseAuthClient:
        return SupabaseAuthClient(
            url=auth_url,
            auto_refresh_token=client_options.auto_refresh_token,
            persist_session=client_options.persist_session,
            storage=client_options.storage,
            headers=client_options.headers,
        )

    @staticmethod
    def _init_postgrest_client(
        rest_url: str,
        headers: Dict[str, str],
        schema: str,
        timeout: Union[int, float, Timeout] = DEFAULT_POSTGREST_CLIENT_TIMEOUT,
    ) -> AsyncPostgrestClient:
        return AsyncPostgrestClient(
            rest_url, headers=headers, schema=schema, timeout=timeout
        )

    def _get_auth_headers(self) -> Dict[str, str]:
        return {
            "apiKey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
        }

    async def _get_token_header(self):
        try:
            access_token = await self.auth.get_session().access_token
        except:
            access_token = self.supabase_key

        return {
            "Authorization": f"Bearer {access_token}",
        }

    def _listen_to_auth_events(self, event: AuthChangeEvent, session):
        if event in ["SIGNED_IN", "TOKEN_REFRESHED", "SIGNED_OUT"]:
            self._postgrest = None
            self._storage = None
            self._functions = None


async def create_client(
    supabase_url: str,
    supabase_key: str,
    options: ClientOptions = ClientOptions(),
) -> AsyncClient:
    client = AsyncClient(supabase_url=supabase_url, supabase_key=supabase_key, options=options)
    await client._init_task
    return client
