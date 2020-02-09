import uuid
import pytest

from redgraph import properties


@pytest.mark.asyncio
async def test_crud(redis_conn):
    andrea = uuid.uuid1()
    nate = uuid.uuid1()
    james = uuid.uuid1()
    mark = uuid.uuid1()
    karen = uuid.uuid1()

    await properties.set(redis_conn, "first_name", "Andrea", andrea)
    await properties.set(redis_conn, "first_name", "Nate", nate)
    await properties.set(redis_conn, "first_name", "James", james)
    await properties.set(redis_conn, "first_name", "Mark", mark)
    await properties.set(redis_conn, "first_name", "Karen", karen)
    await properties.set(
        redis_conn, "last_name", "Dewey", andrea, nate, james, mark, karen
    )
    await properties.set(redis_conn, "age", 10, andrea)
    await properties.set(redis_conn, "age", 8, nate)
    await properties.set(redis_conn, "age", 6, james)
    await properties.set(redis_conn, "age", 3, mark)
    await properties.set(redis_conn, "age", 0.5, karen)
    await properties.set(
        redis_conn, "hobbies", ["gardening", "winning Settlers of Catan"], andrea
    )
    await properties.set(
        redis_conn, "hobbies", ["gaming", {"diablo": "barbarian"}], nate
    )
    await properties.set(
        redis_conn, "hobbies", {"poetry": ["dishwashers", "Mozambiquan"]}, james
    )
    await properties.set(redis_conn, "hobbies", ["programming", "gaming"], mark)
    await properties.set(redis_conn, "hobbies", ["reading", "hiking"], karen)

    assert await properties.get(redis_conn, "first_name", mark, james, nate) == [
        "Mark",
        "James",
        "Nate",
    ]
    assert await properties.get(redis_conn, "last_name", andrea, karen) == [
        "Dewey",
        "Dewey",
    ]
    assert await properties.get(redis_conn, "age", mark, karen) == [3, 0.5]
    hobbies = await properties.get(
        redis_conn, "hobbies", andrea, nate, james, mark, karen
    )
    assert hobbies[0] == ["gardening", "winning Settlers of Catan"]
    assert hobbies[1] == ["gaming", {"diablo": "barbarian"}]
    assert hobbies[2] == {"poetry": ["dishwashers", "Mozambiquan"]}
    assert hobbies[3] == ["programming", "gaming"]
    assert hobbies[4] == ["reading", "hiking"]

    await properties.set(redis_conn, "last_name", "Richards", andrea)
    await properties.remove(redis_conn, "hobbies", mark)

    assert await properties.get(redis_conn, "last_name", andrea) == ["Richards"]
    assert await properties.get(redis_conn, "hobbies", mark) == [None]
