package com.brightcove.rna.bigtable;

import com.brightcove.rna.bigtable.core.EntitySchema;
import com.brightcove.rna.bigtable.core.KeySchema;
import org.apache.avro.generic.IndexedRecord;
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
public interface EntityMapper<E extends IndexedRecord> {

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
     * @param entity The entity which this function will map to a Put instance.
     * @return An HBase Put.
     */
    Put mapFromEntity(E entity);

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
    KeySerDe getKeySerDe();

    /**
     * Gets the entity serde instance for this entity mapper
     *
     * @return The entity serde for the entity mapper
     */
    EntitySerDe<E> getEntitySerDe();
}
