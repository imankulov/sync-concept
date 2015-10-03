


def lru_touch(r, key, value, max_values=10):
    """
    Function to use Redis as LRU store containing as many as max_values elements

    Behind the scenes it will use two variables:

    <key>:store -- as sorted set to keep values along with their scores
    <key>:cnt -- counter to generate next value to score

    :param r: Redis instance
    :param key: Key prefix to store sorted set and counter
    :param value: Value to touch
    :param max_values: Maximum number of values we keep track of
    :return: list of values which have been returned from the key
    """
    script = r.register_script("""
    -- "lru_touch" function

    -- KEYS[1]: the store
    -- KEYS[2]: the counter
    -- ARGV[1]: value to touch
    -- ARGV[2]: max number of values in the store

    redis.call("ZADD", KEYS[1], redis.call("INCR", KEYS[2]), ARGV[1])
    local card = redis.call("ZCARD", KEYS[1])
    local maxkeys = tonumber(ARGV[2])

    if card > maxkeys then
        local elems = redis.call("ZRANGE", KEYS[1], 0, card - maxkeys - 1)
        redis.call("ZREM", KEYS[1], unpack(elems))
        return elems
    else
        return {}
    end
    """)
    return script(keys=[key + ":store", key + ":cnt"], args=[value, max_values])
