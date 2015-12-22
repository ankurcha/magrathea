package com.brightcove.rna.bigtable.core;

/**
 * <p>
 * Exception thrown for dataset-related failures. The root of
 * the dataset exception hierarchy.
 * </p>
 */
public class DatasetException extends RuntimeException {

    public DatasetException(String message) {
        super(message);
    }

    public DatasetException(String message, Throwable t) {
        super(message, t);
    }

}
