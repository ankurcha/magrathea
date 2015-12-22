package com.brightcove.rna.bigtable.core;

import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.List;

/**
 * <p>
 * The strategy used to determine how a dataset is partitioned.
 * </p>
 * <p>
 * A {@code PartitionStrategy} is configured with one or more
 * {@link FieldPartitioner}s upon creation. When a Dataset is configured
 * with a partition strategy, we say that data is partitioned. Any entities
 * written to a partitioned dataset are evaluated with its
 * {@code PartitionStrategy} which, in turn, produces a {@link PartitionKey}
 * that is used by the dataset implementation to select the proper partition.
 * </p>
 */
@Value.Immutable
public abstract class PartitionStrategy {

    @Value.Parameter
    public abstract List<FieldPartitioner> fieldPartitioners();

    /**
     * <p>
     * Construct a partition key with a variadic array of values corresponding to
     * the field partitioners in this partition strategy.
     * </p>
     * <p>
     * It is permitted to have fewer values than field partitioners, in which case
     * all subpartititions in the unspecified parts of the key are matched by the
     * key.
     * </p>
     * <p>
     * Null values are not permitted.
     * </p>
     */
    public PartitionKey partitionKey(Object... values) {
        return ImmutablePartitionKey.of(values);
    }

    /**
     * <p>
     * Construct a partition key for the given entity.
     * </p>
     * <p>
     * This is a convenient way to find the partition that a given entity would be
     * written to, or to find a partition using objects from the entity domain.
     * </p>
     */
    public PartitionKey partitionKeyForEntity(Object entity) {
        return partitionKeyForEntity(entity, null);
    }

    /**
     * <p>
     * Construct a partition key for the given entity, reusing the supplied key if not
     * null.
     * </p>
     * <p>
     * This is a convenient way to find the partition that a given entity would be
     * written to, or to find a partition using objects from the entity domain.
     * </p>
     */
    @SuppressWarnings("unchecked")
    public PartitionKey partitionKeyForEntity(Object entity, @Nullable PartitionKey reuseKey) {
        Preconditions.checkArgument(entity instanceof GenericRecord, "entity must be of type org.apache.avro.generic.GenericRecord");
        PartitionKey key = (reuseKey == null ? newKey() : reuseKey);
        for (int i = 0; i < fieldPartitioners().size(); i++) {
            FieldPartitioner fp = fieldPartitioners().get(i);
            String name = fp.getSourceName();
            Object value = ((GenericRecord) entity).get(name);
            key.set(i, fp.apply(value));
        }
        return key;
    }

    /**
     * Constructs a new {@code PartitionKey} that can be used with this
     * {@code PartitionStrategy}.
     *
     * @return a new {@code PartitionKey}
     */
    private PartitionKey newKey() {
        return ImmutablePartitionKey.of(new Object[fieldPartitioners().size()]);
    }

    /**
     * Return a {@link PartitionStrategy} for subpartitions starting at the given index.
     */
    public PartitionStrategy subpartitionStrategyAt(int startIndex) {
        if (startIndex == 0) {
            return this;
        }
        if (startIndex >= fieldPartitioners().size()) {
            return null;
        }
        return ImmutablePartitionStrategy.of(fieldPartitioners().subList(startIndex, fieldPartitioners().size()));
    }


}
