package com.brightcove.rna.bigtable.avro;

/**
 * An interface to construct AvroRecordBuilders. The avro entity mappers need to be able to create
 * new AvroRecordBuilders of the type they are configured to construct.
 *
 * @param <T> The type of AvroRecord the builder will create.
 */
public interface AvroRecordBuilderFactory<T> {

    /**
     * Get a new AvroRecordBuilder instance.
     *
     * @return The AvroRecordBuilder instance.
     */
    AvroRecordBuilder<T> getBuilder();

    /**
     * Get the class of record the AvroRecordBuilder this factory returns will
     * construct.
     *
     * @return The class.
     */
    Class<T> getRecordClass();
}
