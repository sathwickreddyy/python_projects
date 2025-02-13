## Redis Locking Example

# Leader Election and Execution Flow

This approach ensures that only one instance executes the program at a time

## **Leader Election**
- The `acquire_leader` function ensures that only one instance becomes the leader.
- If an instance successfully becomes the leader, it executes the program and releases leadership after completion.

## **Single Heartbeat**
- The leader sends just one heartbeat (`send_heartbeat`) to indicate it is alive during execution.

## **No Infinite Loop**
- The `monitor_leader` function and infinite loop have been removed.
- Each instance attempts to become the leader once and either executes the program or exits.

## **Clean Up After Execution**
- The `LEADER_KEY` is deleted after execution to ensure no stale leadership remains in Redis.

---

## **Execution Flow**

### Instance 1:
1. Acquires leadership (`LEADER_KEY`).
2. Executes all critical sections (CS1 → CS4).
3. Releases both the program lock (`LOCK_KEY`) and leadership (`LEADER_KEY`).

### Instance 2:
1. Attempts to acquire leadership but fails because Instance 1 already holds it.
2. Exits without executing any critical section.

---

## **How It Handles Scenarios**

### Happy Path:
- One instance becomes the leader and executes all critical sections sequentially.
- Other instances detect that they are not leaders and exit immediately.

### Failure Case (Leader Fails Mid-Execution):
- If the leader fails before completing all critical sections:
    - The `LOCK_KEY` expires after its TTL (30 seconds), allowing another instance to retry execution if needed.
    - Retry logic can be added in other instances to detect expired locks and reattempt execution.

