# First-Come, First-Served (FCFS) Ordering Policy

**Type:** `fcfs-ordering-policy`

> [!NOTE]
> This plugin is enabled by default when flow control is enabled. You do not need to explicitly declare it in your configuration.

The First-Come, First-Served (FCFS) ordering policy selects requests based on their arrival order at the Flow Control layer.

## Why Choose This Policy?

- **Simplicity and Intuitive Behavior:** Requests are processed in the order they arrive, making the system behavior easy to understand and predict.
- **No Special Inputs Required:** Unlike policies that rely on deadlines or priorities, FCFS works without any additional request metadata or headers.
- **Good for Homogeneous Workloads:** When all requests have similar processing costs and importance, FCFS is often the fairest approach.

## What It Does

The FCFS policy compares requests by their **Logical Enqueue Time**—the timestamp when the request first arrived at the `controller.FlowController`. The request with the earliest timestamp is rendered "less" than (i.e., higher priority than) requests that arrived later.

## Inputs consumed

This policy inspects the following attributes of the request item:
- **Logical Enqueue Time**: Used as the arrival timestamp to determine order.

## Behavior

Every flow's queue is a priority queue ordered by the flow's `OrderingPolicy`, so requests are
dispatched in exact logical-arrival order: the request with the earliest logical enqueue time is
always at the head of the queue.

## Configuration

This policy does not require any custom parameters.

```yaml
orderingPolicyRef: fcfs-ordering-policy
```

## Trade-offs

- **Head-of-Line Blocking:** A large request at the front of the queue can delay smaller, more urgent requests behind it.
- **No Urgency Awareness:** It does not consider deadlines or request priority; a request that has already timed out on the client side might still be processed if it was the oldest in the queue.

## Related Documentation
*   [Ordering Overview](../README.md)
*   [Flow Control User Guide](https://github.com/kubernetes-sigs/gateway-api-inference-extension/blob/v1.5.0/site-src/guides/flow-control.md)
