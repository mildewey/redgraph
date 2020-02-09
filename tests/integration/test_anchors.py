import uuid
import pytest

from redgraph import anchors


@pytest.mark.asyncio
async def test_add_rem(redis_conn):
    a = uuid.uuid1()
    b = uuid.uuid1()
    c = uuid.uuid1()
    d = uuid.uuid1()
    anchor = "#test_add_rem"
    await anchors.add(redis_conn, anchor, a, b, c, a, b, a, a, a)
    await anchors.add(redis_conn, anchor, a)
    members = await anchors.members(redis_conn, anchor)
    assert len(members) == 3
    count = await anchors.count(redis_conn, anchor)
    assert count == 3
    assert a in members
    assert b in members
    assert c in members
    assert d not in members
    assert await anchors.is_member(redis_conn, anchor, a)
    assert await anchors.is_member(redis_conn, anchor, b)
    assert await anchors.is_member(redis_conn, anchor, c)
    assert not await anchors.is_member(redis_conn, anchor, d)

    await anchors.remove(redis_conn, anchor, d)
    await anchors.remove(redis_conn, anchor, a)

    members = await anchors.members(redis_conn, anchor)
    assert a not in members
    assert b in members
    assert c in members
    assert d not in members
    assert len(members) == 2


@pytest.mark.asyncio
async def test_ops(redis_conn):
    a = uuid.uuid1()
    b = uuid.uuid1()
    c = uuid.uuid1()
    d = uuid.uuid1()

    ab = "ab"
    abc = "abc"
    a_alone = "a_alone"
    cd = "cd"

    await anchors.add(redis_conn, ab, a, b)
    await anchors.add(redis_conn, abc, a, b, c)
    await anchors.add(redis_conn, a_alone, a)
    await anchors.add(redis_conn, cd, c, d)

    members = await anchors.union(redis_conn, ab, abc)
    assert a in members
    assert b in members
    assert c in members
    assert d not in members

    await anchors.unionstore(redis_conn, "new_abc", ab, abc)
    members = await anchors.members(redis_conn, "new_abc")
    for mem in [a, b, c]:
        assert mem in members
    assert len(members) == 3

    members = await anchors.intersection(redis_conn, ab, cd)
    assert a not in members
    assert b not in members
    assert c not in members
    assert d not in members

    await anchors.interstore(redis_conn, "new_empty", ab, cd)
    members = await anchors.members(redis_conn, "new_empty")
    assert len(members) == 0

    members = await anchors.intersection(redis_conn, ab, a_alone)
    assert a in members
    assert b not in members
    assert c not in members
    assert d not in members

    members = await anchors.difference(redis_conn, ab, a_alone)
    assert a not in members
    assert b in members

    await anchors.diffstore(redis_conn, "b_alone", ab, a_alone)
    members = await anchors.members(redis_conn, "b_alone")
    assert b in members
    assert len(members) == 1

    members = await anchors.difference(redis_conn, a_alone, ab)
    assert len(members) == 0
