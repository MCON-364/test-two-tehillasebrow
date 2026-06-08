package edu.touro.las.mcon364.test2;

import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ══════════════════════════════════════════════════════════════
 * Problem 3 of 3
 * ══════════════════════════════════════════════════════════════
 *
 * A reporting system receives multiple batches of transactions.
 * The batches can be processed independently, and the results must be combined
 * into a single ReportSummary.
 *
 * Your job is to choose an appropriate concurrency design pattern from the ones
 * we studied and apply it correctly.
 *
 * Each inner list represents one batch of transactions.
 *
 * Requirements:
 * - Process multiple batches concurrently.
 * - Each batch must be processed exactly once.
 * - Do not use parallelStream()
 * - Do not use synchronized keyword on methods or blocks.
 * - Track how many batches were actually processed using a thread-safe mechanism
 *   in the integer field numberOfBatchesProcessed
 * - Start all available work before waiting for final results.
 * - Shut down any concurrency resources you create.
 */
public class ParallelReportBuilder {

    /** Simple domain object. Do not modify. */
    public record Transaction(String id, int amount) {}

    // YOUR ERROR: these two records are marked "Do not modify", but you changed their
    // `long` fields to `int`. I reverted them. (totalAmount/counts use long because many
    // batches summed together can overflow a 32-bit int.)

    /** Do not modify. */
    public record BatchStats(long totalAmount,
                             long transactionCount,
                             int maxTransactionAmount,
                             int minTransactionAmount) {}

    /** Do not modify. */
    public record ReportSummary(long totalAmount,
                                long totalCount,
                                int globalMax,
                                int globalMin,
                                int batchesProcessed) {}


    // CORRECT: AtomicInteger is the thread-safe counter required by the spec.
    private final AtomicInteger numberOfBatchesProcessed = new AtomicInteger(0);

    /*
     * TODO 2 — generateReport(List<List<Transaction>> batches, int workers)
     *
     * For each batch, compute:
     * - totalAmount
     * - transactionCount
     * - maxTransactionAmount
     * - minTransactionAmount
     *   (Hint: summaryStatistics())
     * Then combine all BatchStats objects into one ReportSummary.
     */
    public ReportSummary generateReport(List<List<Transaction>> batches, int workers)
            throws InterruptedException, ExecutionException, IllegalArgumentException {

        // 2A: validate inputs. (You only checked workers == 0; negatives and a null
        //     list also need rejecting.)
        if (batches == null || workers <= 0)
            throw new IllegalArgumentException();

        // 2B: a fixed thread pool is the right structure for "many independent tasks".
        ExecutorService pool = Executors.newFixedThreadPool(workers);

        // ───────────────────────────────────────────────────────────────────────
        // YOUR ERRORS in the old submit loop:
        //   int totalAmount = 0; ... globalMax = new AtomicInteger(); ...
        //   for (int i = 0; i < workers; i++) {            // (1) looped over WORKERS, not batches.
        //       List<Transaction> b = batches.get(i);      //     If batches.size() != workers you
        //                                                   //     either miss batches or crash.
        //       Runnable task = () -> {
        //           totalAmount += transaction.amount();   // (2) DOES NOT COMPILE: a lambda cannot
        //           totalCount++;                           //     reassign a local int (captured locals
        //           max = Math.max(...);                    //     must be effectively final).
        //       };                                          // (3) the task was never submitted/run.
        //   }                                               // (4) no BatchStats, no streams, and
        //                                                   //     numberOfBatchesProcessed never updated.
        // ───────────────────────────────────────────────────────────────────────
        // FIX — the Future pattern: submit one Callable<BatchStats> PER BATCH, collect the
        // Futures, THEN combine. Each task computes its own stats (no shared mutable state),
        // which is exactly why this parallelizes safely.

        // 2C: submit one unit of work per batch. submit() returns immediately, so this
        //     loop starts ALL work before we ever wait on a result.
        List<Future<BatchStats>> futures = new ArrayList<>();
        for (List<Transaction> batch : batches) {
            Callable<BatchStats> task = () -> {
                // streams + summaryStatistics() gives sum/count/max/min in one pass.
                IntSummaryStatistics s = batch.stream()
                        .mapToInt(Transaction::amount)
                        .summaryStatistics();
                numberOfBatchesProcessed.incrementAndGet(); // thread-safe progress tracking
                return new BatchStats(s.getSum(), s.getCount(), s.getMax(), s.getMin());
            };
            futures.add(pool.submit(task));
        }

        // 2D: now collect results and fold them into the summary. f.get() blocks until
        //     that batch is done. Start max/min at the opposite extremes so the first real
        //     value always wins (starting at 0 was a bug — it would clamp negatives/positives).
        long totalAmount = 0;
        long totalCount = 0;
        int globalMax = Integer.MIN_VALUE;
        int globalMin = Integer.MAX_VALUE;
        for (Future<BatchStats> f : futures) {
            BatchStats stats = f.get();
            totalAmount += stats.totalAmount();
            totalCount  += stats.transactionCount();
            globalMax = Math.max(globalMax, stats.maxTransactionAmount());
            globalMin = Math.min(globalMin, stats.minTransactionAmount());
        }

        // 2E: release the pool's threads.
        pool.shutdown();

        // 2F: return the combined summary.
        return new ReportSummary(totalAmount, totalCount, globalMax, globalMin,
                numberOfBatchesProcessed.get());
    }

    /*
     * TODO 3 — getProcessedBatchCount()
     *
     * Return the current number of batches processed.
     */
    public int getProcessedBatchCount() {
        // CORRECT: read the atomic counter.
        return numberOfBatchesProcessed.get();
    }
}
