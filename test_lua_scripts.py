import pytest
import redis
import lua_scripts


@pytest.fixture()
def r():
    rr = redis.StrictRedis()
    keys = rr.keys('test*')
    if keys:
        rr.delete(*keys)
    return rr


def test_lru_touch(r):
    assert lua_scripts.lru_touch(r, 'test_foo', 'a', max_values=2) == []
    assert lua_scripts.lru_touch(r, 'test_foo', 'b', max_values=2) == []

    # touch "c", pop "a" as the oldest value
    assert lua_scripts.lru_touch(r, 'test_foo', 'c', max_values=2) == ['a']

    # touch the same value, b is newer than c
    assert lua_scripts.lru_touch(r, 'test_foo', 'b', max_values=2) == []

    assert r.zrange('test_foo:store', 0, -1) == ['c', 'b']
