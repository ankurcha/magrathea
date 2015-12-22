package com.brightcove.rna.bigtable.core;

import org.immutables.value.Value;

/**
 * <p>
 * A key for retrieving partitions from a {@link Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is a ordered sequence of values corresponding to the
 * {@link FieldPartitioner}s in a {@link PartitionStrategy}. A
 * {@link PartitionKey} may be obtained using
 * {@link PartitionStrategy#partitionKey(Object...)} or
 * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
 * </p>
 */
@Value.Immutable
public abstract class PartitionKey {

    @Value.Parameter
    public abstract Object[] values();

    /**
     * Return the value at the specified index in the key.
     */
    public Object get(int index) {
        if (index < values().length) {
            return values()[index];
        }
        return null;
    }

    void set(int index, Object value) {
        values()[index] = value;
    }

    /**
     * Return the number of values in the key.
     */
    public int getLength() {
        return values().length;
    }

}
