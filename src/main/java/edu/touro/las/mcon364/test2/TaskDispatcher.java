package edu.touro.las.mcon364.test2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Problem 2 of 3
 *
 * A TaskDispatcher processes strings using multiple threads. Thread number is limited.
 * Each worker upper-cases the task string and records the result.
 * The results list and the completed counter must always be in sync.
 *
 * TODO 1 — pool
 *   Create a thread pool whose size is capped at POOL_SIZE.
 *
 * TODO 2 — lock
 *   Choose a lock that allows you to explicitly acquire and release it,
 *
 *
 * TODO 3 — dispatch(List<String> tasks)
 *   Hand each task off to the pool. The work each thread does is:
 *     (a) upper-case the string
 *     (b) record the result by calling recordResult()
 *     (c) return the result
 *   Give back a handle to each piece of work so the caller can retrieve
 *   the results later. Do not wait for the results here.
 *
 * TODO 4 — recordResult(String result)
 *   The results list and completedCount must never get out of sync.
 *   Make sure no other thread can come in between updating one and the other.
 *   Always release the lock even if something goes wrong.
 *
 * TODO 5 — shutdown()
 *   Stop accepting new work and wait up to 10 seconds for running tasks to finish.
 *
 * TODO 6 — getResults() / getCompletedCount()
 *   Reads must be guarded the same way writes are.
 *   getResults() must return a copy so callers cannot modify internal state.
 *
 */
public class TaskDispatcher {

    public static final int POOL_SIZE = 4;

    // CORRECT: a fixed thread pool caps the number of worker threads at POOL_SIZE.
    private final ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);

    // CORRECT: ReentrantLock is the Lock that you lock()/unlock() explicitly.
    private final Lock lock = new ReentrantLock();
    // provided — do not change
    private final List<String> results = new ArrayList<>();
    private int completedCount = 0;

    /*
     *   Hand each task off to the pool. The work each thread does is:
     *     (a) upper-case the string
     *     (b) record the result by calling recordResult()
     *     (c) return the result
     *   Give back a handle to each piece of work so the caller can retrieve
     *   the results later. Do not wait for the results here.
     *   You have to use streams!
     */
    public List<Future<String>> dispatch(List<String> tasks) {
        // ───────────────────────────────────────────────────────────────────────
        // YOUR ERROR (was):
        //   return tasks.stream().map(String::toUpperCase).map(this::recordResult).toList();
        //   (1) It never touched `pool`, so NOTHING ran on a worker thread — all the
        //       work happened right here on the calling thread (not "dispatching").
        //   (2) `.map(this::recordResult)` does not compile: map() needs a function that
        //       RETURNS a value, but recordResult returns void.
        //   (3) The method must return List<Future<String>> (a handle per task), and you
        //       were returning a List<String> of the strings themselves.
        // ───────────────────────────────────────────────────────────────────────
        // FIX: for each task, submit a Callable to the pool. pool.submit(...) returns a
        // Future<String> immediately (it does NOT block / wait), which is the "handle"
        // the caller uses later to fetch the result.
        return tasks.stream()
                .map(task -> pool.submit(() -> {
                    String upper = task.toUpperCase(); // (a) upper-case
                    recordResult(upper);               // (b) record it (thread-safe)
                    return upper;                      // (c) return it -> becomes the Future's value
                }))
                .collect(Collectors.toList());
    }

    public void recordResult(String result) {
        // YOUR ERROR: you only added to `results` and NEVER incremented completedCount,
        // so the two fell out of sync — violating the core requirement of this problem.
        // FIX: update BOTH inside the SAME lock so no thread can observe them mismatched.
        lock.lock();
        try {
            results.add(result);
            completedCount++;
        } finally {
            lock.unlock(); // CORRECT: unlocking in finally guarantees release even on exception.
        }
    }

    public void shutdown() throws InterruptedException {
        // YOUR ERROR: you waited 30 seconds; the spec says wait UP TO 10 seconds.
        pool.shutdown();                                // stop accepting new tasks
        pool.awaitTermination(10, TimeUnit.SECONDS);    // wait up to 10s for running tasks
    }

    public List<String> getResults() {
        // Read under the same lock as the writes, and return a copy.
        // NOTE: the test requires the returned list to be UNMODIFIABLE (calling add()
        // on it must throw). new ArrayList<>(results) is a copy but is still modifiable,
        // so we wrap it with List.copyOf to make it immutable.
        lock.lock();
        try {
            return List.copyOf(results);
        } finally {
            lock.unlock();
        }
    }

    public int getCompletedCount() {
        // CORRECT: guarded read, same lock as the writers.
        lock.lock();
        try {
            return completedCount;
        } finally {
            lock.unlock();
        }
    }

}
