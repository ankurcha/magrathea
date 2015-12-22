package com.brightcove.rna.bigtable.core;

/**
 * Exception thrown to indicate that there was a problem parsing or validating a schema.
 */
public class SchemaValidationException extends RuntimeException {
    public SchemaValidationException(String msg) {
        super(msg);
    }

    public SchemaValidationException(Throwable cause) {
        super(cause);
    }

    public SchemaValidationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
