package com.brightcove.rna.bigtable;

import org.apache.avro.generic.IndexedRecord;

/**
 * This class handles key serialization and deserialization.
 */
public interface KeySerDe {
    byte[] serialize(IndexedRecord key);
    IndexedRecord deserialize(byte[] keyBytes);
}
