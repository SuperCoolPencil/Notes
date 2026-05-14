This is a log on how I implement speed limiting in Surge :P
Let's see how it goes. 

After reading about  [Rate Limiting Algorithms](https://www.geeksforgeeks.org/system-design/rate-limiting-algorithms-system-design) for a download manager the best rate limiting algorithm seems to be a simple token bucket algorithm

Reason? Network traffic is rarely stable, packets arrive in bursts, TB allows for slight controlled bursts without dropping packets. 

How does Token Bucket works?

There are 3 main components:
- Bucket (duh): maximum number of tokens, determines burst limit.
- Refill Rate: continuously adds tokens to the bucket
- Consumer: takes away tokens

---

Now let's see what would be the right place to implement this in Surge. We need a per-download as well as a global bucket. Is there a way to smartly combine these?

one more non-functional requirement we need to worry about. We need to be able to change the download speed at any time. It must not be locked after a download has started. 
Hmm... this might be a bit tricky.

I'm thinking of implementing a pay-later scheme where the bytes will be read off the network first and then we will wait for tokens to come. I'm thinking of something like this:
- ThrottledReader reads 64KB from TCP socket
- ThrottledReader sees rate limit (32KB/s)
- ThrottledReader makes goroutine sleep for 2s
  
---

Now let's see how this can be implemented.

So here's how the code looks currently for reading bytes off the network:
```go
for readSoFar < int(readSize) {
	n, err := resp.Body.Read(buf[readSoFar:readSize])
	if n > 0 {
		readSoFar += n
		activeTask.LastActivity.Store(time.Now().UnixNano())
	}
	if err != nil {
		readErr = err
		break
	}
	if n == 0 {
		readErr = io.ErrUnexpectedEOF
		break
	}
}

```

We cannot directly implement TB here using a wrapper for resp.Body.Read 
because the Health Monitor will start killing healthy workers.

We need some sort of a hook, that hooks back into the health monitor and pings it saying "Wait I'm sleeping cause of rate limit, don't kill me."

But how to ensure the right slow workers are killed too. Hmm.... 

We need to differentiate between:
1. **A throttled worker:** Slow because _we_ told it to sleep. (Healthy)
2. **A stalled worker:** Slow because the _remote server_ is crawling. (Unhealthy)

we need to measure how long a worker spends waiting for `net/http` and completely ignore the time spent as a part of rate limiting.

let me write a snippet and see how it feels:
```go
// WorkerStats is shared between the ThrottledReader and the Health Monitor
type WorkerStats struct {
	BytesRead       atomic.Int64
	NetworkTimeNano atomic.Int64 // Only tracks time spent waiting on the socket
	LastActivity    atomic.Int64
}

type ThrottledReader struct {
	r      io.Reader
	global *rate.Limiter
	local  *rate.Limiter
	ctx    context.Context
	stats  *WorkerStats
}

func (tr *ThrottledReader) Read(p []byte) (int, error) {
	// 1. Start the stopwatch
	start := time.Now()
	// 2. Hit the actual TCP socket
	n, err := tr.r.Read(p)
	// 3. Stop the stopwatch. This is ONLY the time the network took.
	networkDuration := time.Since(start)
	if n > 0 {
		// Update our metrics concurrently
		tr.stats.BytesRead.Add(int64(n))
		tr.stats.NetworkTimeNano.Add(networkDuration.Nanoseconds())
		tr.stats.LastActivity.Store(time.Now().UnixNano())
		// Enforce speed limits AFTER we've recorded the network time.
		// The monitor doesn't care how long these WaitN calls take.
		if tr.local != nil {
			tr.local.WaitN(tr.ctx, n)
		}
		if tr.global != nil {
			tr.global.WaitN(tr.ctx, n)
		}
	}
	return n, err
}
```

`rate.Limiter` has `SetLimit()` and `SetBurst()`, so non-functional requirement of changing speed mid-download is already satisfied without any extra work. 

and we will also need to change the health check. the current health check has been implemented like: 

```go
// Check for absolute stall: no data received for StallTimeout
// This catches dead connections that the relative speed check misses
lastActivity := active.LastActivity.Load()
if lastActivity > 0 {
	timeSinceData := now.Sub(time.Unix(0, lastActivity))
	if timeSinceData >= stallTimeout {
		utils.Debug("Health: Worker %d stalled (no data for %v), cancelling",
			workerID, timeSinceData.Truncate(time.Millisecond))
		if active.Cancel != nil {
			active.Cancel()
		}
		continue // Already cancelled, skip speed check
	}
}

// Check for slow worker (relative speed)
// Only cancel if: below threshold
if meanSpeed > 0 {
	workerSpeed := active.GetSpeed()
	threshold := d.Runtime.GetSlowWorkerThreshold()
	isBelowThreshold := workerSpeed > 0 && workerSpeed < threshold*meanSpeed
	if isBelowThreshold {
		utils.Debug("Health: Worker %d slow (%.2f KB/s vs mean %.2f KB/s), cancelling",
		workerID, workerSpeed/float64(types.KB), meanSpeed/float64(types.KB))
		if active.Cancel != nil {
			active.Cancel()
			}
		}
}

```

**The slow worker check will false-positive on throttled workers**

A throttled worker can still be stalled (remote server dying, connection dropping). We just can't compare it against _other workers_ because the comparison is meaningless.

The fix is to change _what you compare against_ based on whether the worker is throttled:
```go
workerSpeed := active.GetSpeed()

if active.IsThrottled.Load() {
    // Can't compare against mean — other workers may be unthrottled.
    // Instead, check if the worker is achieving its own throttle rate.
    // If it's well below the limit we set, the remote server is the bottleneck.
    assignedLimit := active.ThrottleLimit.Load() // bytes/sec
    if assignedLimit > 0 && workerSpeed < assignedLimit * slowThrottledFactor {
        // genuinely slow, not just us being conservative
        active.Cancel()
    }
} else {
    // Original relative check against mean
    isBelowThreshold := workerSpeed > 0 && workerSpeed < threshold*meanSpeed
    if isBelowThreshold {
        active.Cancel()
    }
}
```

The logic: if we told the worker to run at 32KB/s and it's only hitting 8KB/s, it's not us:
the remote is crawling. `slowThrottledFactor` can be something like `0.5`.

---

I'm gonna create a new file to be shared by `SingleDownloader` and `ConcurrentDownloader`:
throttle.go with implementation of `ThrottledReader`

This change is gonna be as minimal as possible.

