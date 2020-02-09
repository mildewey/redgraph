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

    await properties.set(redis_conn, andrea, first_name="Andrea")
    await properties.set(redis_conn, nate, first_name="Nate")
    await properties.set(redis_conn, james, first_name="James")
    await properties.set(redis_conn, mark, first_name="Mark")
    await properties.set(redis_conn, karen, first_name="Karen")
    await properties.set(
        redis_conn, andrea, nate, james, mark, karen, last_name="Dewey",
    )
    await properties.set(redis_conn, andrea, age=10)
    await properties.set(redis_conn, nate, age=8)
    await properties.set(redis_conn, james, age=6)
    await properties.set(redis_conn, mark, age=3)
    await properties.set(redis_conn, karen, age=0.5)
    await properties.set(
        redis_conn, andrea, hobbies=["gardening", "winning Settlers of Catan"]
    )
    await properties.set(redis_conn, nate, hobbies=["gaming", {"diablo": "barbarian"}])
    await properties.set(
        redis_conn, james, hobbies={"poetry": ["dishwashers", "Mozambiquan"]}
    )
    await properties.set(redis_conn, mark, hobbies=["programming", "gaming"])
    await properties.set(redis_conn, karen, hobbies=["reading", "hiking"])

    assert await properties.property(redis_conn, "first_name", mark, james, nate) == [
        "Mark",
        "James",
        "Nate",
    ]
    assert await properties.property(redis_conn, "last_name", andrea, karen) == [
        "Dewey",
        "Dewey",
    ]
    assert await properties.property(redis_conn, "age", mark, karen) == [3, 0.5]
    hobbies = await properties.property(
        redis_conn, "hobbies", andrea, nate, james, mark, karen
    )
    assert hobbies[0] == ["gardening", "winning Settlers of Catan"]
    assert hobbies[1] == ["gaming", {"diablo": "barbarian"}]
    assert hobbies[2] == {"poetry": ["dishwashers", "Mozambiquan"]}
    assert hobbies[3] == ["programming", "gaming"]
    assert hobbies[4] == ["reading", "hiking"]

    await properties.set(redis_conn, andrea, last_name="Richards")
    await properties.remove(redis_conn, "hobbies", mark)

    assert await properties.property(redis_conn, "last_name", andrea) == ["Richards"]
    assert await properties.property(redis_conn, "hobbies", mark) == [None]
    assert await properties.entity(
        redis_conn, mark, "first_name", "last_name", "age"
    ) == dict(first_name="Mark", last_name="Dewey", age=3)
