package com.brightcove.rna.bigtable.core;

import org.immutables.value.Value;

@Value.Immutable
public abstract class PartitionKey {

    @Value.Parameter public abstract Object[] values();
    @Value.Derived   public int length() { return values().length; }

    /**
     * Return the value at the specified index in the key.
     */
    public Object get(int index) {
        if (index >= values().length) {
            return null;
        }
        return values()[index];
    }

    void set(int index, Object value) {
        values()[index] = value;
    }

}
