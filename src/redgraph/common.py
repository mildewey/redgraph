import uuid
from functools import singledispatch
from typing import Union, List, Mapping

import aioredis

from redgraph.types import Handle


@singledispatch
def handle(*parts) -> Handle:
    return b":".join([bytes(str(part), "utf-8") for part in parts])


@handle.register
def _(*parts: bytes) -> Handle:
    return b":".join(parts)


@handle.register
def _(*parts: str) -> Handle:
    return b":".join([bytes(part, "utf-8") for part in parts])
