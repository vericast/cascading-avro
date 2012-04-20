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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import static org.junit.Assert.assertEquals;

/**
 * Class CascadingFieldsGeneratorTest
 */
public class CascadingFieldsGeneratorTest {
    @Test
    public void testGenerate() throws Exception {
        final CascadingFieldsGenerator gen = new CascadingFieldsGenerator();
        final Schema schema = getSchema();

        final Writer writer = new StringWriter();
        gen.generate(schema, writer);
        writer.close();

        final String actual = writer.toString();
        final String expected = Resources.toString(getClass().getResource("expected.txt"), Charsets.UTF_8);

        assertEquals(expected, actual);
    }

    @Test
    public void testDestFile() throws Exception {
        final CascadingFieldsGenerator gen = new CascadingFieldsGenerator();
        final Schema schema = getSchema();

        final File actual = gen.getDestination(schema, new File("/tmp"));
        assertEquals(new File("/tmp/com/maxpoint/cascading/avro/Test1Fields.java"), actual);
    }

    private Schema getSchema() throws IOException {
        final Schema.Parser parser = new Schema.Parser();
        return parser.parse(getClass().getResourceAsStream("test1.avsc"));
    }
}
