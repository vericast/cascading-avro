/*
* Copyright (c) 2012 MaxPoint Interactive, Inc. All Rights Reserved.
*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.maxpoint.cascading.avro;

import cascading.scheme.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedHashMap;

/**
 * A variant of {@link TextDelimited} scheme that gets field name and type information from an Avro schema.
 */
@SuppressWarnings("deprecation")
public class TextScheme extends AvroSchemeBase {
    public static final EnumSet<Schema.Type> ALLOWED_TYPES = EnumSet.of(Schema.Type.BOOLEAN,
            Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.LONG,
            Schema.Type.NULL, Schema.Type.STRING, Schema.Type.UNION);

    private final TextDelimited text;

    /**
     * Creates TAB-separated scheme
     */
    public TextScheme(Schema avroSchema) {
        this(avroSchema, "\t", null);
    }

    /**
     * Creates scheme with given delimiter
     */
    public TextScheme(Schema avroSchema, String delimiter) {
        this(avroSchema, delimiter, null);
    }

    /**
     * Creates scheme with given delimiter and quoting character
     */
    public TextScheme(Schema avroSchema, String delimiter, String quote) {
        final LinkedHashMap<String, FieldType> schemaFields = parseSchema(avroSchema, ALLOWED_TYPES);
        final Fields fields = fields(schemaFields);
        setSinkFields(fields);
        setSourceFields(fields);

        text = new TextDelimited(fields, delimiter, quote, inferClasses(schemaFields.values()));
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        text.sinkInit(tap, conf);
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) {
        text.sourceInit(tap, conf);
    }

    @Override
    public Tuple source(Object key, Object value) {
        return text.source(key, value);
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        text.sink(tupleEntry, outputCollector);
    }
}
