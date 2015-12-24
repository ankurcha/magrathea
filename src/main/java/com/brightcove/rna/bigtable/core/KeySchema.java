package com.brightcove.rna.bigtable.core;

/**
 * The KeySchema type.
 */
public class KeySchema {
    private final String rawSchema;
    public KeySchema(String rawSchema) {
        this.rawSchema = rawSchema;
    }
    public String getRawSchema() {
        return rawSchema;
    }
}
