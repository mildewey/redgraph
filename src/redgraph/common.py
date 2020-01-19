import uuid
import json
from functools import singledispatch, reduce
from typing import Union, List, Mapping

import aioredis

from redgraph.types import Handle, Value, Document, Field, Key


class BadTypeError(Exception):
    pass


@singledispatch
def handle(*parts) -> Handle:
    return b":".join([bytes(str(part), "utf-8") for part in parts])


@handle.register
def _(*parts: bytes) -> Handle:
    return b":".join(parts)


@handle.register
def _(*parts: str) -> Handle:
    return b":".join([bytes(part, "utf-8") for part in parts])


def serialize(val: Value) -> bytes:
    return bytes(json.dumps(val), "utf-8")


def deserialize(val: str) -> Value:
    return json.loads(val)


def extract(entity: Document, *keys: Key) -> Value:
    try:
        return reduce(lambda e, i: e[i], keys, entity)
    except Exception as e:
        msg = f"Unable to extract value for keys {'.'.join([str(key) for key in keys])} from entity {entity}"
        logger.warning(msg)
        logger.exception(e)
        raise BadTypeError(msg)
