import uuid
import json
import logging
from functools import singledispatch, reduce
from typing import Union, List, Mapping

import aioredis

from redgraph.types import Handle


logger = logging.getLogger("redgraph")


def handle(*parts) -> Handle:
    return b":".join([bytes(str(part), "utf-8") for part in parts])
