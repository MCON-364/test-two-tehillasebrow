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

    // TODO: initialise this field with a thread-safe Map implementation
    //       — which Map implementation from the lesson guarantees thread-safe reads and writes?
    private final ConcurrentMap<String, Integer> stock = new ConcurrentHashMap<>();

    // TODO: declare and initialise a private final field called totalUnitsAdded that tracks the
    //       running total of units ever added, thread-safely, without using synchronized
private final AtomicInteger totalUnitsAdded= new AtomicInteger(0);

    /**
     * Adds {@code qty} units of {@code item} to inventory.
     *
     * @param item the item name (non-null, non-blank)
     * @param qty  number of units to add (must be > 0)
     * @throws IllegalArgumentException if qty ≤ 0
     */
    public void addStock(String item, int qty) {
        // TODO: validate qty > 0
            if(qty<=0)
                throw new IllegalArgumentException();
        // TODO: atomically add qty to the item's current stock
        //       Hint: the thread-safe Map implementation you chose has a merge() method
        //             that can do this in one atomic step
            else{
               stock.merge(item,qty,Integer::sum ); //finisne this later
            }
        // TODO: atomically add qty to totalUnitsAdded
            totalUnitsAdded.addAndGet(qty);
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
        // TODO: validate qty > 0
        if(qty<=0)
            throw new IllegalArgumentException();

        // TODO: atomically check-and-decrement.
        //       If current stock >= qty, subtract qty.
        Integer currStock=stock.get(item);
        if (currStock>=qty){
               stock.compute(item,(qty(i->{i-qty});
                       return true;
            }
        //       Otherwise, leave stock unchanged.
        else
        //       Return true if stock was depleted, false if unchanged

        //       Hint: your chosen Map has a compute() method that lets you
        //             read and write in one atomic step.

        return false; //placeholder
    }

    /**
     * Returns the current stock for {@code item}, or 0 if unknown.
     */
    public int getStock(String item) {
       return stock.get(item); //placeholder
    }

    /**
     * Returns the cumulative number of units ever added (all items combined).
     */
    public int getTotalUnitsAdded() {
        return totalUnitsAdded.get(); //placeholder
    }

    /**
     * Returns an unmodifiable snapshot of the current inventory.
     * Callers cannot use the returned map to change internal state.
     */
    public Map<String, Integer> getSnapshot() {
        // TODO: return a defensive copy
        return stock; //placeholder
    }
}

