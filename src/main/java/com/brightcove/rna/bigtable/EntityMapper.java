package com.brightcove.rna.bigtable;

import com.brightcove.rna.bigtable.core.EntitySchema;
import com.brightcove.rna.bigtable.core.KeySchema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.Set;

/**
 * An interface for mapping HBase Result instances to a StorageKey/Entity pair,
 * and a StorageKey/Entity to an HBase Put instances.
 * <p>
 * EntityMapper instances should be state-less so they can be reused across multiple Result and Entity instances.
 * They should encapsulate in one place the mapping of business entities to and from HBase.
 *
 * @param <E> The entity type
 */
public interface EntityMapper<K extends IndexedRecord, E extends IndexedRecord> {

    /**
     * Map an HBase Result instance to an Entity of type T. Retrieve the StorageKey from
     * the result instance as well, and wraps both in an KeyEntity instance. This
     * KeyEntity instance is returned.
     *
     * @param result The HBase result instance representing a row from an HBase table.
     * @return A KeyEntity instance which wraps a StorageKey and an Entity of type T.
     */
    E mapToEntity(Result result);

    /**
     * Map an entity of type T to an HBase Put instance.
     *
     * @param key Row key object
     * @param entity The entity which this function will map to a Put instance.
     * @return An HBase Put.
     */
    Put mapFromEntity(K key, E entity);

    /**
     * Maps a StorageKey, fieldName and an increment value to an HBase Increment instance
     * that will increment the value in the cell pointed to by fieldName.
     *
     * @param key       a key to use to construct the Increment instance.
     * @param fieldName The name of the field we are incrementing
     * @param amount    The amount to increment the field by
     * @return An HBase Increment
     */
    Increment mapToIncrement(K key, String fieldName, long amount);

    /**
     * Maps the result of an increment to the new value of the field that was
     * incremented.
     *
     * @param result    The HBase client Result object that contains the increment result
     * @param fieldName The name of the field we are getting the increment result for
     * @return The new field value.
     */
    long mapFromIncrementResult(Result result, String fieldName);

    /**
     * Gets the set of required HBase columns that we would expect to be in the
     * result.
     *
     * @return The set of required columns.
     */
    Set<String> getRequiredColumns();

    /**
     * Gets the set of required column families that must exist in the HBase table
     * we would be mapping from.
     *
     * @return The set of required column families.
     */
    Set<String> getRequiredColumnFamilies();

    /**
     * Gets the key schema instance for this entity mapper.
     *
     * @return The HBaseCommonKeySchema instance.
     */
    KeySchema getKeySchema();

    /**
     * Gets the entity schema instance for this entity mapper.
     *
     * @return The HBaseCommonEntitySchema instance.
     */
    EntitySchema getEntitySchema();

    /**
     * Gets the key serde instance for this entity mapper
     *
     * @return The key serde for the entity mapper
     */
    KeySerDe<K> getKeySerDe();

    /**
     * Gets the entity serde instance for this entity mapper
     *
     * @return The entity serde for the entity mapper
     */
    EntitySerDe<E> getEntitySerDe();
}
