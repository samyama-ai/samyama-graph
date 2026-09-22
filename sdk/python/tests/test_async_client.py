"""The async surface does not block the event loop (API-12, #1436).

Checking that `samyama_aio.AsyncSamyamaClient.query` is a coroutine would
check the wrong half. A coroutine that blocks is exactly the failure this
module exists to rule out, and it passes every shape test there is: it is
awaitable, it is declared `async def`, it returns the right answer, and it
stalls every other task in the process while it runs.

So the central case here measures the event loop instead. A ticker task
increments a counter every millisecond; a query runs beside it; the counter is
read afterwards. If the loop was blocked the counter barely moves.

Measured on this repository before the extension released the GIL: a 1.28 s
query let another Python thread advance **once**. After: 1,310 times. The
threshold below is set far under that so the case is about the difference
between blocked and not blocked rather than about how fast the host is.

These tests need the compiled extension, so they run in the nightly
`python-extension` job rather than in PR CI, the same as `test_embedded.py`.

Each case is a plain synchronous test that calls `asyncio.run` on an inner
coroutine, rather than using `pytest-asyncio`. The SDK's test dependencies are
`pytest` and what the package already needs; adding a plugin so the tests can
be written a little more neatly would make the nightly job fail on a machine
that has the extension but not the plugin.
"""

import asyncio
import sys
import time

import pytest

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))

from samyama_aio import AsyncSamyamaClient  # noqa: E402

#: Nodes to build the slow query over. 3,000 gives a self-join of nine million
#: comparisons, which is seconds rather than milliseconds -- long enough for a
#: blocked loop to be unmistakable and short enough to sit in a test suite.
NODES = 3000

SLOW_QUERY = "MATCH (a:N), (b:N) WHERE a.v = b.v RETURN count(*) AS c"


async def _seeded():
    db = await AsyncSamyamaClient.embedded()
    await db.query(f"UNWIND range(1, {NODES}) AS i CREATE (:N {{v: i}})")
    return db


def test_a_query_returns_the_same_answer_as_the_blocking_client():
    async def case():
        db = await _seeded()
        got = await db.query_readonly(SLOW_QUERY)
        want = db.sync.query_readonly(SLOW_QUERY)
        assert len(got) == len(want) == 1
        assert got.records == want.records
        # The count is the diagonal of the self-join: every node matches itself.
        assert got.records[0][0] == NODES

    asyncio.run(case())


def test_the_event_loop_keeps_running_during_a_query():
    async def case():
        db = await _seeded()

        ticks = 0
        stop = False

        async def ticker():
            nonlocal ticks
            while not stop:
                ticks += 1
                await asyncio.sleep(0.001)

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.2)  # let the ticker reach a steady rate
        before = ticks

        started = time.monotonic()
        await db.query_readonly(SLOW_QUERY)
        elapsed = time.monotonic() - started

        during = ticks - before
        stop = True
        await task

        assert elapsed > 0.2, (
            f"the query finished in {elapsed:.3f}s, too fast for this to measure "
            f"anything. Raise NODES."
        )
        # A loop that is running manages on the order of elapsed/0.001 ticks. A
        # blocked one manages 0 or 1. Ten is far below the first and far above the
        # second, so the case does not turn into a speed test of the host.
        assert during >= 10, (
            f"the event loop advanced {during} times during a {elapsed:.2f}s query. "
            f"It is blocked: either the async wrapper is not using a thread, or the "
            f"extension is holding the GIL across the call (#1436)."
        )

    asyncio.run(case())


def test_two_queries_overlap():
    async def case():
        # The consequence of the above, stated as the thing a user notices: two
        # concurrent queries take less than the sum of their times. With the GIL
        # held they serialise exactly.
        db = await _seeded()

        started = time.monotonic()
        await db.query_readonly(SLOW_QUERY)
        one = time.monotonic() - started

        started = time.monotonic()
        await asyncio.gather(
            db.query_readonly(SLOW_QUERY),
            db.query_readonly(SLOW_QUERY),
        )
        two = time.monotonic() - started

        assert two < 1.8 * one, (
            f"two concurrent queries took {two:.2f}s against {one:.2f}s for one. "
            f"They are serialising rather than overlapping."
        )

    asyncio.run(case())


def test_the_client_is_an_async_context_manager():
    async def case():
        async with await AsyncSamyamaClient.embedded() as db:
            await db.query("CREATE (:P {name: 'a'})")
            result = await db.query_readonly("MATCH (n:P) RETURN count(n) AS c")
            assert result.records[0][0] == 1

    asyncio.run(case())


def test_an_error_surfaces_as_an_exception_not_a_hung_await():
    async def case():
        db = await AsyncSamyamaClient.embedded()
        with pytest.raises(RuntimeError):
            await db.query_readonly("THIS IS NOT CYPHER")
        # And the client still works afterwards: the worker thread is not poisoned.
        assert await db.ping() == "PONG"

    asyncio.run(case())


def test_cancelling_an_await_returns_control_to_the_loop():
    async def case():
        # What cancellation does and does not do. The await stops; the query does
        # not -- Python cannot interrupt a thread inside a C call, and the engine
        # has no cancellation API (#1393). The test asserts the half that is true
        # rather than the half the name suggests.
        db = await _seeded()
        task = asyncio.create_task(db.query_readonly(SLOW_QUERY))
        await asyncio.sleep(0.05)
        task.cancel()

        started = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await task
        returned_in = time.monotonic() - started

        assert returned_in < 0.5, (
            f"cancellation took {returned_in:.2f}s to return control; it should be "
            f"immediate even though the query underneath keeps running"
        )

    asyncio.run(case())
