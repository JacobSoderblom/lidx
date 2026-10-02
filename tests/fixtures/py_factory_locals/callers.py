from typing import Optional
import models


def _make_coordinator(*, client=None) -> Coordinator:
    return Coordinator()


def _make_opt() -> Optional[OptFoo]:
    return OptFoo()


async def _make_async() -> AsyncFoo:
    return AsyncFoo()


def _make_dotted() -> models.DottedFoo:
    return models.DottedFoo()


def _make_str() -> "StrFoo":
    return StrFoo()


def _make_unann():
    return UnannFoo()


def _make_list() -> list[ListFoo]:
    return [ListFoo()]


def _make_a() -> MixedA:
    return MixedA()


def _make_b() -> MixedB:
    return MixedB()


def test_via_factory():
    coord = _make_coordinator(client=None)
    coord.handle_trigger()


def test_via_direct_ctor():
    coord2 = Coordinator()
    coord2.handle_trigger()


def test_optional():
    foo = _make_opt()
    foo.opt_run()


async def test_async():
    foo = await _make_async()
    foo.async_run()


def test_dotted():
    foo = _make_dotted()
    foo.dotted_run()


def test_string_ref():
    foo = _make_str()
    foo.str_run()


def test_unannotated():
    foo = _make_unann()
    foo.unann_run()


def test_list():
    foos = _make_list()
    foos.list_run()


def test_reassigned():
    thing = _make_a()
    thing = _make_b()
    thing.mixed_run()


def _make_pipe() -> PipeFoo | None:
    return PipeFoo()


def _make_awaitable() -> Awaitable[CoroFoo]:
    return None


async def _make_unawaited() -> UnawaitedFoo:
    return UnawaitedFoo()


def _make_dup() -> DupFoo:
    return DupFoo()


def _make_dup() -> int:
    return 1


def _make_shadow() -> ShadowFoo:
    return ShadowFoo()


def _make_twin() -> pkg_a.twin.Twin:
    return pkg_a.twin.Twin()


def test_pipe_none():
    foo = _make_pipe()
    foo.pipe_run()


async def test_awaitable():
    foo = await _make_awaitable()
    foo.coro_run()


async def test_unawaited():
    foo = _make_unawaited()
    foo.unawaited_run()


def test_duplicate_def():
    foo = _make_dup()
    foo.dup_run()


def test_shadowed(_make_shadow):
    foo = _make_shadow()
    foo.shadow_run()


def test_twin():
    twin = _make_twin()
    twin.twin_run()
