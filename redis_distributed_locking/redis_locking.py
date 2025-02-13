import redis
import time
import socket

# Connect to Redis server
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Unique identifier for this instance (e.g., hostname)
instance_id = socket.gethostname()

# Keys for Redis
LEADER_KEY = "leader"
HEARTBEAT_KEY = f"heartbeat:{instance_id}"
LOCK_KEY = "program_lock"

def acquire_leader():
    """
    Attempt to become the leader by setting a unique value in Redis.
    """
    return redis_client.set(LEADER_KEY, instance_id, nx=True, ex=10)  # Leader expires in 10 seconds

def send_heartbeat():
    """
    Send periodic heartbeat to indicate this instance is alive.
    """
    redis_client.set(HEARTBEAT_KEY, "alive", ex=5)  # Heartbeat expires in 5 seconds

def acquire_program_lock():
    """
    Acquire a lock for the entire program execution.
    """
    return redis_client.set(LOCK_KEY, instance_id, nx=True, ex=30)  # Lock expires in 30 seconds

def release_program_lock():
    """
    Release the program lock if owned by this instance.
    """
    script = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
    """
    redis_client.eval(script, 1, LOCK_KEY, instance_id)

def execute_program():
    """
    Execute critical sections of the program as the leader.
    """
    if acquire_program_lock():
        try:
            print(f"{instance_id} acquired program lock. Executing critical sections...")
            # Simulate critical sections (CS1 → CS4)
            time.sleep(5)  # Simulate CS1
            print(f"{instance_id} completed CS1.")
            time.sleep(5)  # Simulate CS2
            print(f"{instance_id} completed CS2.")
            time.sleep(5)  # Simulate CS3
            print(f"{instance_id} completed CS3.")
            time.sleep(5)  # Simulate CS4
            print(f"{instance_id} completed CS4.")
        except Exception as e:
            print(f"Error during execution: {e}")
        finally:
            release_program_lock()
            print(f"{instance_id} released program lock.")
    else:
        print(f"{instance_id} could not acquire program lock. Another instance is executing.")

# Main logic: Single execution flow
if __name__ == "__main__":
    if acquire_leader():
        print(f"{instance_id} is elected as the leader!")
        send_heartbeat()  # Send a single heartbeat to indicate leadership
        execute_program()  # Execute critical sections as leader
        # Clean up leader key after execution
        redis_client.delete(LEADER_KEY)
        print(f"{instance_id} has finished execution and released leadership.")
    else:
        print(f"{instance_id} is not the leader. Exiting...")
