import asyncio
import ipaddress
import socket
import time

from .errors import UnsafeTarget

_ok = {}


async def ensure_public(host):
    now = time.monotonic()
    until = _ok.get(host)
    if until and until > now:
        return
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(
            host, None, type=socket.SOCK_STREAM
        )
    except socket.gaierror:
        return
    for info in infos:
        address = ipaddress.ip_address(info[4][0].split("%")[0])
        if not address.is_global:
            raise UnsafeTarget(f"{host} resolves to a non-public address")
    _ok[host] = now + 300