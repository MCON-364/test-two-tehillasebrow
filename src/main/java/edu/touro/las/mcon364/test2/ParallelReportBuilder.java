package edu.touro.las.mcon364.test2;

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


    // TODO 1: declare and initialize private thread-safe progress tracking state called numberOfBatchesProcessed
        private final AtomicInteger numberOfBatchesProcessed= new AtomicInteger(0);
    /*
     * TODO 2 — generateReport(List<List<Transaction>> batches, int workers)
     *
     * For each batch, compute:
     * - totalAmount
     * - transactionCount
     * - maxTransactionAmount
     * - minTransactionAmount
     *   (Hint: summaryStatistics())
     * Then combine all BatchStats objects into one ReportSummary containing:
     * - total amount across all batches
     * - total number of transactions
     * - global maximum transaction amount
     * - global minimum transaction amount
     * - number of batches processed
     *
     * Think carefully about:
     * - which concurrency pattern best matches independent tasks
     * - which java.util.concurrent classes support that pattern
     * - how to safely update shared progress
     * - how to avoid waiting too early
     * - how to handle empty batches or an empty input list
     *
     */

    public ReportSummary generateReport(List<List<Transaction>> batches, int workers)
            throws InterruptedException, ExecutionException, IllegalArgumentException {

        // TODO 2A: validate inputs where appropriate
    if (workers == 0)
        throw new IllegalArgumentException();
        // TODO 2B: create the concurrency structure needed for the pattern you chose
        ExecutorService pool = Executors.newFixedThreadPool(workers);

        // TODO 2C: submit or assign one unit of work per batch
        // Each unit of work should:
        // - compute BatchStats for that batch
        // - safely record that one more batch has been processed
        // - you have to use streams here
        AtomicInteger totalAmount = new AtomicInteger();
        AtomicInteger totalCount = new AtomicInteger();
        AtomicInteger globalMax = new AtomicInteger();
        AtomicInteger globalMin = new AtomicInteger();

        for (int i = 0; i < workers; i++) {
            List<Transaction> b=batches.get(i);
            Runnable  task = () -> {
              for(Transaction transaction: b ){
                  totalAmount.addAndGet(transaction.amount());
                  totalCount.getAndIncrement();
                  globalMax=Math.max(totalAmount, globalMax);
                  globalMin=Math.min(globalMin, totalAmount)
              }

            };


        }

        // TODO 2D: after all work has been started, collect results
        // and combine them into the summary variables above
        // you don't have to use streams here. In this case for loop is acceptable



        // TODO 2E: shut down any concurrency resources you created
        pool.shutdown();
        // TODO 2F: return the completed ReportSummary
        return  new ReportSummary( totalAmount, totalCount, globalMax, globalMin, numberOfBatchesProcessed.get()); //placeholder
    }

    /*
     * TODO 3 — getProcessedBatchCount()
     *
     * Return the current number of batches processed.
     */
    public int getProcessedBatchCount() {
       return numberOfBatchesProcessed.get(); //placeholder
    }
}
