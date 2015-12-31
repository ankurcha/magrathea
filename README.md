# Magrathea

This repository contains a library to provide the functionality to convert [Avro](http://avro.apache.org)
types to/from [HBase](http://hbase.apache.org) compatible format.

# Usage

To add this library to a project

```gradle

repositories {
    maven {
        url 'http://nexus.vidmark.local:8081/nexus/content/repositories/releases/'
    }
}

dependencies {
   compile 'com.brightcove.rns:magrathea:VERSION'
}

```


# HBase serialization and deserialization support

As of version `0.0.5`, the `rna/schema` adds helper classes that allow the user to convert avro types to corresponding
HBase Puts which can then be used to update a HBase (or Google Cloud Bigtable) table. The main class that enables this
functionality is `AvroEntityMapper` which should be constructed using `AvroEntityMapperFactory`.

## Writing

The following code snippet depicts the use of the AvroEntityMapper to convert a collection of avro objects to HBase
`Put` instances using the `EntityMaper#mapFromEntity` method and subsequently writing it to a HBase table.

```java

        // initialize HBase/BigTable connection (optional)
        Connection conn = ...
        TableName tableName = TableName.valueOf("rowTable");

        // initialize to the entity mapper
        AvroEntityMapper<Row> mapper = AvroEntityMapperFactory.createEntityMapper(Row.class);

        input.stream()
            // process the stream of input to a stream of Puts
            .map(mapper::mapFromEntity)
            // process the stream of Puts to actual writes to HBase or Google Cloud BigTable
            .forEach(put -> {
                            try (Table table = conn.getTable(tableName)) {
                                table.put(put);
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        });

```

## Reading

The following code snippet depicts the use of the AvroEntityMapper to read ( `Scan` )  rows from a HBase table to
generate a List of `Result` instances followed by converting them to Row objects using the `EntityMapper#mapToEntity`
method.

```java

        // initialize HBase/BigTable connection (optional)
        Connection conn = ...
        TableName tableName = TableName.valueOf("rowTable");

        // initialize to the entity mapper
        AvroEntityMapper<Row> mapper = AvroEntityMapperFactory.createEntityMapper(Row.class);

        try (Table table = conn.getTable(tableName)) {
            // perform table scan
            Iterable<Result> scanner = table.getScanner(new Scan());
            // iterate over the results
            StreamSupport.stream(scanner.spliterator(), false)
                         // map to entity objects and collect them into a list
                         .map(mapper::mapToEntity)
                         .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

```

# Field Mapping

The mapping of the fields to the HBase columns and row key is determined by the **field mapping** that is provided as a
part of the the Avro schema json.

Each field must have a mapping annotation, which specifies how that field is mapped to an HBase column.
Allowed mapping types are `key`, `column` and `keyAsColumn`.

* The `key` mapping type on a field tells the entity mapper to map that field to the row key. The value field should
contain the field's ordering in the row key.
* The `column` mapping type on a field tells the entity mapper to map that field to the fully_qualified_column.
* The `keyAsColumn` mapping type on a field tells the entity mapper to map each key of the value type to a column in
the specified column_family. This annotation is only allowed on map and record types.

Here is an example schema:

```json
{
    "name": "record_name",
    "type": "record",
    "fields": [
     {
        "name": "keyPart1",
        "type": "string",
        "mapping": { "type": "key", "value": "1" }
     },
     {
        "name": "keyPart0",
        "type": "long",
        "mapping": { "type": "key", "value": "0" }
     },
     {
        "name": "field1",
        "type": "int",
        "mapping": { "type": "column", "value": "meta:field1" }
     },
     {
        "name": "field2",
        "type": { "type": "map", "values": "string" },
        "mapping": { "type": "keyAsColumn", "value": "map_family" }
     },
     {
        "name": "field3",
        "type": {
            "type": "record",
            "name": "record1",
            "fields": [
                { "name": "sub_field1", "type": "int"},
                { "name": "sub_field2", "type": "int"},
            ]
        },
        "mapping": { "type": "keyAsColumn", "value": "record_family:" }
     }
    ]
}
```

Here the fields of the record are serialized as follows:

| Field Name | Location | Remarks |
| ---------- | -------- | ------- |
| `keyPart0` | Row key | This field is serialized using a mem-cmp compatible encoding and is present as the first part of the row key (based on the `value` field of the `mapping`). |
| `keyPart1` | Row key | This field is serialized using a mem-cmp compatible encoding and is present as the first part of the row key (based on the `value` field of the `mapping`). |
| `field1` | column | Serialized using a modified version of the standard encoding (to enable increment operations on numeric types) and stored in the `meta` column family at the `field1` qualifier. |
| `field2` | column family | Stored as a columns family `map_family` with each of the individual keys becoming the qualifiers and the corresponding values stored under them in their standard serialized form. |
| `field3` | column family | Stored as a columns family `record_family` with each of the individual field names becoming the qualifiers and the corresponding values stored under them in their standard serialized form. |


# Attribution

The EntityMapper and related classes under the `com.brightcove.rna.bigtable` package have been heavily inspired by
[KiteSDK](https://github.com/kite-sdk/kite) and attempts have been made where ever possible, to maintain compatibility
of the output so that they can be interchanged if need arises.


# What's with the name ?

Magrathea is an ancient planet located in orbit around the twin suns Soulianis and Rahm in the
heart of the Horsehead Nebula.

It was the Magratheans who constructed the planet-sized computer named Earth (for a race of hyperintelligent
pandimensional beings, the mice) to determine the ultimate question of Life, the Universe, and Everything,
which is required to understand the Answer to Life, the Universe, and Everything.
