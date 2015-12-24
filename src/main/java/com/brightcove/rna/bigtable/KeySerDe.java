package com.brightcove.rna.bigtable;

import org.apache.avro.generic.IndexedRecord;

/**
 * This class handles key serialization and deserialization.
 */
public interface KeySerDe<K extends IndexedRecord> {
    byte[] serialize(K key);
    K deserialize(byte[] keyBytes);
}
