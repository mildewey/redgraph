import uuid

import pytest

from redgraph import graph


@pytest.fixture
async def small_graph(redis_conn):
    small = dict(
        a=uuid.uuid1(),
        b=uuid.uuid1(),
        c=uuid.uuid1(),
        d=uuid.uuid1(),
        e=uuid.uuid1(),
        ab=uuid.uuid1(),
        bc=uuid.uuid1(),
        db=uuid.uuid1(),
        be=uuid.uuid1(),
        ed=uuid.uuid1(),
    )
    await graph.relate(redis_conn, small["a"], small["ab"], small["b"])
    await graph.relate(redis_conn, small["b"], small["bc"], small["c"])
    await graph.relate(redis_conn, small["d"], small["db"], small["b"])
    await graph.relate(redis_conn, small["b"], small["be"], small["e"])
    await graph.relate(redis_conn, small["e"], small["ed"], small["d"])
    yield small
    await graph.unrelate(redis_conn, small["a"], small["ab"], small["b"])
    await graph.unrelate(redis_conn, small["b"], small["bc"], small["c"])
    await graph.unrelate(redis_conn, small["d"], small["db"], small["b"])
    await graph.unrelate(redis_conn, small["b"], small["be"], small["e"])
    await graph.unrelate(redis_conn, small["e"], small["ed"], small["d"])


@pytest.mark.asyncio
async def test_sp(redis_conn, small_graph):
    predicates = await graph.sp(redis_conn, small_graph["a"])
    assert len(predicates) == 1
    assert small_graph["ab"] in predicates


@pytest.mark.asyncio
async def test_so(redis_conn, small_graph):
    objects = await graph.so(redis_conn, small_graph["b"])
    assert len(objects) == 2
    assert small_graph["c"] in objects
    assert small_graph["e"] in objects


@pytest.mark.asyncio
async def test_ps(redis_conn, small_graph):
    subjects = await graph.ps(redis_conn, small_graph["bc"])
    assert len(subjects) == 1
    assert small_graph["b"] in subjects


@pytest.mark.asyncio
async def test_po(redis_conn, small_graph):
    objects = await graph.po(redis_conn, small_graph["bc"])
    assert len(objects) == 1
    assert small_graph["c"] in objects


@pytest.mark.asyncio
async def test_os(redis_conn, small_graph):
    subjects = await graph.os(redis_conn, small_graph["b"])
    assert len(subjects) == 2
    assert small_graph["a"] in subjects
    assert small_graph["d"] in subjects


@pytest.mark.asyncio
async def test_op(redis_conn, small_graph):
    predicates = await graph.op(redis_conn, small_graph["d"])
    assert len(predicates) == 1
    assert small_graph["ed"] in predicates


@pytest.mark.asyncio
async def test_spo(redis_conn, small_graph):
    objects = await graph.spo(redis_conn, small_graph["a"], small_graph["ab"])
    assert len(objects) == 1
    assert small_graph["b"] in objects


@pytest.mark.asyncio
async def test_pso(redis_conn, small_graph):
    objects = await graph.pso(redis_conn, small_graph["ab"], small_graph["a"])
    assert len(objects) == 1
    assert small_graph["b"] in objects


@pytest.mark.asyncio
async def test_sop(redis_conn, small_graph):
    predicates = await graph.sop(redis_conn, small_graph["e"], small_graph["d"])
    assert len(predicates) == 1
    assert small_graph["ed"] in predicates


@pytest.mark.asyncio
async def test_osp(redis_conn, small_graph):
    predicates = await graph.osp(redis_conn, small_graph["d"], small_graph["e"])
    assert len(predicates) == 1
    assert small_graph["ed"] in predicates


@pytest.mark.asyncio
async def test_pos(redis_conn, small_graph):
    subjects = await graph.pos(redis_conn, small_graph["bc"], small_graph["c"])
    assert len(subjects) == 1
    assert small_graph["b"] in subjects


@pytest.mark.asyncio
async def test_ops(redis_conn, small_graph):
    subjects = await graph.ops(redis_conn, small_graph["c"], small_graph["bc"])
    assert len(subjects) == 1
    assert small_graph["b"] in subjects
