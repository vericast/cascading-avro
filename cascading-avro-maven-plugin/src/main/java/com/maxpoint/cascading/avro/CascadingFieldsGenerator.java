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

import org.apache.avro.Schema;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * Class CascadingFieldsGenerator
 */
public class CascadingFieldsGenerator {
    private final VelocityEngine engine;
    private final String path = '/' + getClass().getPackage().getName().replace('.', '/') + '/';

    public CascadingFieldsGenerator() {
        engine = new VelocityEngine();
        engine.addProperty("resource.loader", "class");
        engine.addProperty("class.resource.loader.class", ClasspathResourceLoader.class.getName());
        engine.setProperty("runtime.references.strict", true);
        engine.init();
    }

    public void generate(Schema schema, Writer output) throws IOException {
        final VelocityContext context = new VelocityContext();
        context.put("schema", schema);
        context.put("this", this);

        engine.mergeTemplate(path + "fields.vm", "UTF-8", context, output);
    }

    public String fieldsClassName(String name) {
        return name + "Fields";
    }

    public File getDestination(Schema schema, File outputDirectory) {
        final String classFile = fieldsClassName(schema.getName()) + ".java";
        final String packageDir;
        if(schema.getNamespace() == null) {
            packageDir = "";
        } else {
            packageDir = schema.getNamespace().replace('.', '/');
        }
        return new File(new File(outputDirectory, packageDir), classFile);
    }
}
