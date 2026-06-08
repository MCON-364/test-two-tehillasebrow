package edu.touro.las.mcon364.test2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ══════════════════════════════════════════════════════════════
 *  Problem 1 of 3
 * ══════════════════════════════════════════════════════════════
 *
 * SCENARIO
 * --------
 * A warehouse system lets many threads update item stock simultaneously.
 * You must make the {@code InventoryManager} thread-safe so that no stock
 * updates are lost and stock never goes negative.
 *
 * REQUIREMENTS  (read every TODO carefully — each is graded)
 *
 * 1. {@code addStock(String item, int qty)}
 *    - Adds {@code qty} units of {@code item} to the inventory.
 *    - {@code qty} must be > 0; throw {@link IllegalArgumentException} otherwise.
 *    - Must be thread-safe under concurrent calls.
 *    - Also increments the running {@code totalUnitsAdded} counter atomically.
 *
 * 2. {@code removeStock(String item, int qty)}
 *    - Removes {@code qty} units of {@code item} if sufficient stock exists.
 *    - Returns {@code true} if the removal succeeded, {@code false} if the
 *      current stock is less than {@code qty} (do NOT go negative).
 *    - {@code qty} must be > 0; throw {@link IllegalArgumentException} otherwise.
 *    - Must be thread-safe: two threads must not both succeed when only one
 *      unit of stock remains and both try to remove one unit.
 *    - Hint: once you have chosen the right Map implementation, its
 *      {@code compute()} method lets you read and write atomically.
 *
 * 3. {@code getStock(String item)}
 *    - Returns the current stock for {@code item}, or {@code 0} if the item
 *      has never been added.
 *
 * 4. {@code getTotalUnitsAdded()}
 *    - Returns the running total of every unit ever added across all items.
 *    - Must reflect concurrent additions accurately but you cannot mark the method as synchronized.
 *
 * 5. {@code getSnapshot()}
 *    - Returns an unmodifiable copy of the current inventory so callers
 *      cannot mutate internal state.
 *    - Hint: {@link Map#copyOf(Map)}.
 *
 *
 * DO NOT use any other concurrency utilities.
 */
public class InventoryManager {

    // CORRECT (you had this right): ConcurrentHashMap gives thread-safe reads/writes,
    // and its merge()/compute() methods run atomically (no lost updates).
    private final ConcurrentMap<String, Integer> stock = new ConcurrentHashMap<>();

    // CORRECT (you had this right): AtomicInteger lets us add concurrently
    // without the `synchronized` keyword.
    private final AtomicInteger totalUnitsAdded = new AtomicInteger(0);

    /**
     * Adds {@code qty} units of {@code item} to inventory.
     *
     * @param item the item name (non-null, non-blank)
     * @param qty  number of units to add (must be > 0)
     * @throws IllegalArgumentException if qty ≤ 0
     */
    public void addStock(String item, int qty) {
        // Reject bad input first. (You had this right.)
        if (qty <= 0)
            throw new IllegalArgumentException();

        // merge() = "if absent put qty, else combine old+qty". Integer::sum does old+qty.
        // The whole read-modify-write happens atomically inside the map. (You had this right.)
        stock.merge(item, qty, Integer::sum);

        // Atomically bump the global counter. (You had this right.)
        totalUnitsAdded.addAndGet(qty);
        // NOTE: I removed your `else { ... }` around merge(). It worked, but since the
        // `if` branch throws, the `else` is unnecessary — flatter code is easier to read.
    }

    /**
     * Removes {@code qty} units of {@code item} if sufficient stock exists.
     *
     * @param item the item name
     * @param qty  number of units to remove (must be > 0)
     * @return {@code true} if removal succeeded; {@code false} if insufficient stock
     * @throws IllegalArgumentException if qty ≤ 0
     */
    public boolean removeStock(String item, int qty) {
        if (qty <= 0)
            throw new IllegalArgumentException();

        // ───────────────────────────────────────────────────────────────────────
        // YOUR ERRORS HERE (this whole block):
        //   Integer currStock = stock.get(item);          // (1) NullPointerException if item
        //                                                  //     was never added (get() returns null,
        //                                                  //     then `currStock >= qty` unboxes null).
        //   if (currStock >= qty) {                        // (2) NOT ATOMIC: you read with get(), then
        //       stock.compute(item, currStock(i->i-qty))); //     write with compute() as a SEPARATE step.
        //       return true;                               //     Another thread can change stock in between,
        //   }                                              //     so two threads can both "succeed" on the
        //                                                  //     last unit -> stock goes negative.
        //                                                  // (3) `currStock(i->i-qty)` is not valid Java —
        //                                                  //     compute() needs a (key,value)->newValue
        //                                                  //     BiFunction, and the parentheses/semicolons
        //                                                  //     were unbalanced (it didn't compile).
        // ───────────────────────────────────────────────────────────────────────
        // FIX: do the check AND the decrement inside ONE compute() call so the whole
        // read-modify-write is a single atomic step. A 1-element boolean array lets the
        // lambda report back whether it actually removed anything (a lambda can't assign
        // to a plain local variable, but it CAN mutate the contents of an array).
        boolean[] removed = {false};
        stock.compute(item, (key, current) -> {
            if (current != null && current >= qty) {
                removed[0] = true;
                return current - qty;     // enough stock: subtract
            }
            return current;               // not enough (or absent): leave unchanged
        });
        return removed[0];
    }

    /**
     * Returns the current stock for {@code item}, or 0 if unknown.
     */
    public int getStock(String item) {
        // YOUR ERROR: `return stock.get(item);` returns null for an unknown item,
        // and auto-unboxing null into an int throws NullPointerException.
        // FIX: getOrDefault returns 0 when the item was never added (the spec).
        return stock.getOrDefault(item, 0);
    }

    /**
     * Returns the cumulative number of units ever added (all items combined).
     */
    public int getTotalUnitsAdded() {
        // CORRECT: just read the atomic counter. (You had this right.)
        return totalUnitsAdded.get();
    }

    /**
     * Returns an unmodifiable snapshot of the current inventory.
     * Callers cannot use the returned map to change internal state.
     */
    public Map<String, Integer> getSnapshot() {
        // YOUR ERROR: `return stock;` handed the caller the LIVE internal map, so
        // they could mutate your private state (e.g. snapshot.clear()). It's also
        // not "unmodifiable" as required.
        // FIX: Map.copyOf makes an immutable defensive copy.
        return Map.copyOf(stock);
    }
}
