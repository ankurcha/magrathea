package com.brightcove.rna.bigtable.avro;

import com.brightcove.rna.bigtable.core.DatasetException;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * An AvroRecordBuilderFactory instance that can construct AvroRecordBuilders
 * which are able to build Avro SpecificRecord types.
 *
 * @param <T> The type of SpecificRecord this factory creates builders for.
 */
public class SpecificAvroRecordBuilderFactory<T extends SpecificRecord> implements AvroRecordBuilderFactory<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SpecificAvroRecordBuilderFactory.class);

    private final Class<T> recordClass;
    private final Constructor<T> recordClassConstructor;

    /**
     * Construct the factory, giving it the class of the SpecificRecord the
     * builders will construct.
     *
     * @param recordClass The class of the SpecificRecords the builders will construct.
     */
    public SpecificAvroRecordBuilderFactory(Class<T> recordClass) {
        this.recordClass = recordClass;
        try {
            // Get the constructor of the class so we don't have to perform this expensive reflection call for every
            // builder constructed.
            this.recordClassConstructor = recordClass.getConstructor();
        } catch (Exception e) {
            // A number of reflection exceptions could be caught here. No good way to handle these types of exceptions,
            // so throw an DatasetException up to the user.
            String msg = "Could not get a default constructor for class: " + recordClass.toString();
            LOG.error(msg, e);
            throw new DatasetException(msg, e);
        }
    }

    /**
     * The AvroRecordBuiler instance that can construct SpecificRecords.
     *
     * @param <T> The concrete type of SpecificRecord
     */
    private static class SpecificAvroRecordBuilder<T extends SpecificRecord> implements AvroRecordBuilder<T> {

        private T specificRecord;

        /**
         * Constructor takes the constructor of the SpecificRecord, and will use
         * that to create a new SpecificRecord instance.
         *
         * @param recordConstructor The SpecificRecord constructor.
         */
        public SpecificAvroRecordBuilder(Constructor<T> recordConstructor) {
            try {
                specificRecord = recordConstructor.newInstance();
            } catch (Exception e) {
                // A large number of reflection errors can occur here. There
                // is really no proper way to handle this type of error, so
                // throw a RuntimeException up to the user.
                LOG.error("Could not create a SpecificRecord instance.", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void put(String field, Object value) {
            int fieldPos = specificRecord.getSchema().getField(field).pos();
            specificRecord.put(fieldPos, value);
        }

        @Override
        public T build() {
            return specificRecord;
        }
    }

    @Override
    public AvroRecordBuilder<T> getBuilder() {
        try {
            return new SpecificAvroRecordBuilder<T>(recordClassConstructor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<T> getRecordClass() {
        return recordClass;
    }
}
