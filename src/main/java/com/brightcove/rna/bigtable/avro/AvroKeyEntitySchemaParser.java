/**
 * Copyright 2013 Cloudera Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brightcove.rna.bigtable.avro;

import com.aol.cyclops.matcher.builders.Matching;
import com.aol.cyclops.trycatch.Try;
import com.brightcove.rna.bigtable.KeyEntitySchemaParser;
import com.brightcove.rna.bigtable.core.FieldMapping;
import com.brightcove.rna.bigtable.core.ImmutableFieldMapping;
import com.brightcove.rna.bigtable.core.MappingType;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.brightcove.rna.bigtable.core.MappingType.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This implementation parses Avro schemas for both the key and entities. The
 * entities contain metadata in annotations of the Avro record and Avro record
 * fields.
 *
 * Each field must have a mapping annotation, which specifies how that field is
 * mapped to an HBase column.
 *
 * Allowed mapping types are "key", "column" and "keyAsColumn".
 *
 * The `column` mapping type on a field tells this entity mapper to map that field
 * to the fully_qualified_column.
 *
 * The `keyAsColumn` mapping type on a field tells the entity mapper to map each
 * key of the value type to a column in the specified column_family. This
 * annotation is only allowed on map and record types.
 *
 * The entity record can contain a transactional annotation that tells HBase
 * Common that this entity takes part in transactions
 *
 * The entity record should also contain a tables annotation, which tells HBase
 * Common which tables this entity can be persisted to.
 *
 * Here is an example schema:
 *
 * <pre>
 *
 * {
 *   "name": "record_name",
 *   "type": "record",
 *   "tables": ["table1", "table2"],
 *   "transactional": "true",
 *   "fields": [
 *     {
 *       "name": "field1",
 *       "type": "int",
 *       "mapping": { "type": "column", "value": "meta:field1" }
 *     },
 *
 *     {
 *       "name": "field2",
 *       "type": { "type": "map", "values": "string" },
 *       "mapping": { "type": "keyAsColumn": "value": "map_family" }
 *     }
 *
 *   ]
 * }
 *
 * </pre>
 *
 * An Avro instance of this schema would have its field1 value encoded to the
 * meta:field1 column. Each key/value pair of the field2 map type would have its
 * value mapped to the map_family:[key] column. It will also participate in
 * transactions.
 */
public class AvroKeyEntitySchemaParser implements KeyEntitySchemaParser<AvroKeySchema, AvroEntitySchema> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Schema.Parser avroSchemaParser = new Schema.Parser();

    @Override
    public AvroKeySchema parseKeySchema(String rawSchema) {
        JsonNode schemaAsJson = rawSchemaAsJsonNode(rawSchema);
        Schema schema = avroSchemaParser.parse(rawSchema);
        List<FieldMapping> fieldMappings = getFieldMappings(schemaAsJson, schema);
        List<FieldMapping> keyFieldMappings = fieldMappings.stream()
                                                           .filter(fieldMapping -> fieldMapping.mappingType() == KEY)
                                                           .collect(Collectors.toList());
        return new AvroKeySchema(schema, rawSchema, keyFieldMappings);
    }

    @Override
    public AvroEntitySchema parseEntitySchema(String rawSchema) {
        JsonNode schemaAsJson = rawSchemaAsJsonNode(rawSchema);
        Schema schema = avroSchemaParser.parse(rawSchema);
        List<FieldMapping> fieldMappings = getFieldMappings(schemaAsJson, schema);
        List<String> tables = getTables(schemaAsJson);
        return new AvroEntitySchema(tables, schema, rawSchema, fieldMappings);
    }

    private List<FieldMapping> getFieldMappings(JsonNode schemaAsJson, Schema schema) {
        // Get the mapping of fields to default values.
        Map<String, Object> defaultValueMap = AvroUtils.getDefaultValueMap(schema);
        JsonNode fields = schemaAsJson.get("fields");
        checkNotNull(fields, "Avro Record Schema must contain fields");

        // Build the fieldMappingMap, which is a mapping of field names to AvroFieldMapping instances
        // (which describe the mapping type of the field).
        List<FieldMapping> fieldMappings = Lists.newArrayList();
        for (JsonNode recordFieldJson : fields) {
            String fieldName = recordFieldJson.get("name").getTextValue();
            Schema.Type type = schema.getField(fieldName).schema().getType();
            FieldMapping fieldMapping = createFieldMapping(fieldName, recordFieldJson, defaultValueMap, type);
            if (fieldMapping != null) {
                fieldMappings.add(fieldMapping);
            }
        }

        return fieldMappings;
    }

    private JsonNode rawSchemaAsJsonNode(String rawSchema) {
        return Try.catchExceptions(IOException.class)
                  .tryThis(() -> mapper.readValue(rawSchema, JsonNode.class))
                  .orElse(null);
    }

    /**
     * Given a JsonNode representation of an avro record field, return the
     * AvroFieldMapping instance of that field. This instance contains the type of
     * mapping, and the value of that mapping, which will tell the mapping how to
     * map the field to columns in HBase.
     *
     * @param fieldName The name of the field
     * @param recordFieldJson The Avro record field as a JsonNode.
     * @param defaultValueMap The mapping of fields to default values. Use this to look up possible default value.
     * @param type The field's java type
     * @return The AvroFieldMapping of this field.
     */
    private FieldMapping createFieldMapping(String fieldName, JsonNode recordFieldJson, Map<String, Object> defaultValueMap, Schema.Type type) {
        JsonNode mappingNode = recordFieldJson.get("mapping");
        if (mappingNode == null) {
            return null;
        }

        JsonNode mappingTypeNode = mappingNode.get("type");
        checkNotNull(mappingTypeNode, "mapping attribute must contain type.");

        String mappingValue = mappingNode.get("value").getTextValue();
        MappingType mappingType = Matching
            .when().isValue("column").thenApply(s -> {
                checkNotNull(mappingValue, "column mapping type must contain a value");
                return COLUMN;
            })
            .when().isValue("keyAsColumn").thenApply(s -> {
                checkNotNull(mappingValue, "keyAsColumn mapping type must contain a value");
                return KEY_AS_COLUMN;
            })
            .when().isValue("counter").thenApply(s -> {
                checkArgument(type == Schema.Type.INT || type == Schema.Type.LONG, "counter mapping type must be an int or a long");
                checkNotNull(mappingValue, "counter mapping type must contain a value.");
                return COUNTER;
            })
            .when().isValue("key").thenApply(s -> {
                checkNotNull(mappingValue, "key mapping type must contain an integer value specifying it's key order.");
                return KEY;
            })
            .match(mappingTypeNode.getTextValue())
            .orElse(null);

        if (mappingType == null) {
            return null;
        }

        return ImmutableFieldMapping.builder()
                                    .fieldName(fieldName)
                                    .mappingType(mappingType)
                                    .mappingValue(mappingValue)
                                    .defaultValue(defaultValueMap.get(fieldName))
                                    .build();
    }

    private List<String> getTables(JsonNode avroRecordSchemaJson) {
        if (avroRecordSchemaJson.get("tables") == null) {
            return Lists.newArrayList();
        }
        List<String> result = Lists.newArrayListWithCapacity(avroRecordSchemaJson.get("tables").size());
        for (Iterator<JsonNode> it = avroRecordSchemaJson.get("tables").getElements(); it.hasNext(); ) {
            result.add(it.next().getTextValue());
        }
        return result;
    }
}
