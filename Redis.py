import redis
import json
import time

# Configure logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisDB:

    def __init__(self):
        self.redis_conn = redis.StrictRedis(host="localhost", port=6379, db=0)
        logging.info("Connected to Redis")

    def redis_set_value(self, key, value):
        """Set a string value in Redis."""
        conn = self.redis_conn
        result = conn.set(key, value)
        logging.info(f"Set value for key: {key}")
        return result

    

    def redis_set_value_and_expiry(self, key, value, expiry):
        """Set a string value in Redis with an expiration time."""
        conn = self.redis_conn
        result = conn.setex(key, expiry, value)
        logging.info(f"Set value for key: {key} with expiry: {expiry} seconds")
        return result

   
   

    def redis_delete_value(self, key):
        """Delete a value from Redis."""
        conn = self.redis_conn
        result = conn.delete(key)
        logging.info(f"Deleted key: {key}")
        return result

    def redis_get_value(self, key):
        """Get a string value from Redis."""
        conn = self.redis_conn
        start_time = time.time()
        value = conn.get(key)
        if value:
            value = value.decode('utf-8')
        elapsed_time = time.time() - start_time
        logging.info(f"Retrieved value for key: {key} in {elapsed_time:.4f} seconds")
        return value

   
   


# Usage example
if __name__ == "__main__":
    db = RedisDB()

    # db.redis_set_value("name", "vinay")
    # print(db.redis_get_value("name"))

    # db.redis_set_value_and_expiry("name","vinay",10)
    # print(db.redis_get_value("name"))
    # db.redis_delete_value('key1')
    # print(f"Deleted key 'key1', new value: {redis_db.redis_get_value('key1')}")
