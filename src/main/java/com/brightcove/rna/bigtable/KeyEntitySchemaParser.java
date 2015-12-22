package com.brightcove.rna.bigtable;


import com.brightcove.rna.bigtable.core.EntitySchema;
import com.brightcove.rna.bigtable.core.KeySchema;

/**
 * Interface for the HBase Common StorageKey and Entity parser.
 *
 * @param <KS>    The type that the key schema gets parsed to. Extends KeySchema<RAW_SCHEMA>
 * @param <ES> The type that the raw entity schema gets parsed to. Extends EntitySchema<RAW_SCHEMA>
 */
public interface KeyEntitySchemaParser<KS extends KeySchema, ES extends EntitySchema> {
    KS parseKeySchema(String schema);
    ES parseEntitySchema(String schema);
}
