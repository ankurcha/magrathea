package com.brightcove.rna.bigtable.core;

import com.google.common.base.Function;

import java.util.Comparator;

/**
 * <p>
 * Partitions values for a named field.
 * </p>
 * <p>
 * Used by a {@link PartitionStrategy} to calculate which partition an entity
 * belongs in, based on the value of a given field, called the source field. A field
 * partitioner can, in some cases, provide meaningful cardinality hints to query
 * systems. A good example of this is a hash partitioner which always knows the number of
 * buckets produced by the function.
 * </p>
 * <p>
 * Implementations of {@link FieldPartitioner} are immutable.
 * </p>
 *
 * @param <S> The type of the source field in the entity. The partition function must
 *            accept values of this type.
 * @param <T> The type of the target field, which is the type of the return value of the
 *            partition function.
 * @see PartitionStrategy
 */
public abstract class FieldPartitioner<S, T> implements Function<S, T>, Comparator<T> {

    private final String sourceName;
    private final String name;
    private final Class<S> sourceType;
    private final Class<T> type;

    protected FieldPartitioner(String name, Class<S> sourceType, Class<T> type) {
        this(name, name, sourceType, type);
    }

    protected FieldPartitioner(String sourceName, String name, Class<S> sourceType, Class<T> type) {
        this.sourceName = sourceName;
        this.name = name;
        this.sourceType = sourceType;
        this.type = type;
    }

    /**
     * @return the name of the partition field. Note that the partition field is derived
     * from {@link #getSourceName()} and does not appear in the dataset entity.
     */
    public String getName() {
        return name;
    }

    /**
     * @return the name of the field from which the partition field is derived.
     * @since 0.3.0
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * <p>
     * Apply the partition function to the given {@code value}.
     * </p>
     * <p>
     * The type of value must be compatible with the field partitioner
     * implementation. Normally, this is validated at the time of initial
     * configuration rather than at runtime.
     * </p>
     */
    @Override
    public abstract T apply(S value);

    /**
     * <p>
     * Retrieve the value for the field from the string representation.
     * </p>
     *
     * @since 0.3.0
     * @deprecated will be removed in 0.11.0
     */
    @Deprecated
    public abstract T valueFromString(String stringValue);

    /**
     * <p>
     * Retrieve the value for the field formatted as a {@link String}. By default,
     * this is the object's {@link Object#toString()} representation,
     * but some {@link FieldPartitioner}s may choose to provide a different representation.
     * </p>
     *
     * @since 0.4.0
     * @deprecated will be removed in 0.11.0
     */
    @Deprecated
    public String valueToString(T value) {
        return value.toString();
    }

    /**
     * <p>
     * The type of the source field, which is the type of the type expected by
     * the apply function.
     * </p>
     *
     * @since 0.8.0
     */
    public Class<S> getSourceType() {
        return sourceType;
    }

    /**
     * <p>
     * The type of the target field, which is the type of the return value of the
     * partition function.
     * </p>
     *
     * @since 0.8.0
     */
    public Class<T> getType() {
        return type;
    }
}
