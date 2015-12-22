package com.brightcove.rna.bigtable.core;

/**
 * The supported Mapping Types, which control how an entity field maps to
 * columns in an HBase table.
 */
public enum MappingType {
    KEY,            // Maps a value to a part of the row key
    COLUMN,         // Maps a value to a single column.
    KEY_AS_COLUMN,  // Maps a map or record value to columns in a column family.
    COUNTER         // Maps a field to one that can be incremented
}
