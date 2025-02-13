from redis import Redis


class RedisManager:
    def __init__(self, redis_endpoint, port=6379):
        self.redis_client = Redis(redis_endpoint, port)

    def acquire_lock(self, key, value, ttl):
        """
        Acquire a distributed lock in Redis.
        """
        return self.redis_client.set(key, value, nx=True, ex=ttl)

    def release_lock(self, key, value):
        """
        Release a distributed lock if owned by this instance.
        """
        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        self.redis_client.eval(script, 1, key, value)

    def get_value(self, key):
        """
        Get the value of a Redis key.
        """
        return self.redis_client.get(key)

    def set_value(self, key, value, ttl):
        """
        Set a Redis key with a TTL.
        """
        self.redis_client.set(key, value, ex=ttl)

    def delete(self, key):
        """
        Delete a Redis key.
        """
        self.redis_client.delete(key)