package com.brightcove.rna.bigtable;

import org.apache.avro.generic.IndexedRecord;

/**
 * This class handles key serialization and deserialization.
 */
public interface KeySerDe<KS extends IndexedRecord> {
    byte[] serialize(KS key);
    KS deserialize(byte[] keyBytes);
}
