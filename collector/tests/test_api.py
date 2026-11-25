import asyncio

import pytest

from lively.polite.errors import Blocked
from lively.polite.guard import ensure_public


@pytest.mark.parametrize("host", ["127.0.0.1", "10.0.0.5", "169.254.169.254", "192.168.1.10"])
def test_private_addresses_are_blocked(host):
    with pytest.raises(Blocked):
        asyncio.run(ensure_public(host))


def test_public_address_is_allowed():
    asyncio.run(ensure_public("93.184.216.34"))