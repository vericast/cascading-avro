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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Class CascadingFieldsMojo
 *
 * @goal fields
 * @phase generate-sources
 */
public class CascadingFieldsMojo extends AbstractAvroMojo {
    /**
     * @parameter expression="${sourceDirectory}"
     *            default-value="${basedir}/src/main/avro"
     */
    private File sourceDirectory;

    /**
     * @parameter expression="${outputDirectory}"
     *            default-value="${project.build.directory}/generated-sources/avro"
     */
    private File outputDirectory;

    /**
     * @parameter expression="${sourceDirectory}"
     *            default-value="${basedir}/src/test/avro"
     */
    private File testSourceDirectory;

    /**
     * @parameter expression="${outputDirectory}"
     *            default-value="${project.build.directory}/generated-test-sources/avro"
     */
    private File testOutputDirectory;

    /**
     * A set of Ant-like exclusion patterns used to prevent certain files from
     * being processed. By default, this set is empty such that no files are
     * excluded.
     *
     * @parameter
     */
    protected String[] excludes = new String[0];

    /**
     * A set of Ant-like exclusion patterns used to prevent certain files from
     * being processed. By default, this set is empty such that no files are
     * excluded.
     *
     * @parameter
     */
    protected String[] testExcludes = new String[0];

    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern
     * <code>**&#47;*.avsc</code> is used to select grammar files.
     *
     * @parameter
     */
    private String[] includes = new String[] { "**/*.avsc" };

    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern
     * <code>**&#47;*.avsc</code> is used to select grammar files.
     *
     * @parameter
     */
    private String[] testIncludes = new String[] { "**/*.avsc" };

    private final CascadingFieldsGenerator generator;

    public CascadingFieldsMojo() {
        this.generator = new CascadingFieldsGenerator();
    }

    @Override
    protected String[] getTestExcludes() {
        return testExcludes;
    }

    @Override
    protected String[] getExcludes() {
        return excludes;
    }

    @Override
    protected File getTestOutputDirectory() {
        return testOutputDirectory;
    }

    @Override
    protected File getOutputDirectory() {
        return outputDirectory;
    }

    @Override
    protected File getTestSourceDirectory() {
        return testSourceDirectory;
    }

    @Override
    protected File getSourceDirectory() {
        return sourceDirectory;
    }

    @Override
    protected String[] getIncludes() {
        return includes;
    }

    @Override
    protected String[] getTestIncludes() {
        return testIncludes;
    }

    @Override
    protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
        final File src = new File(sourceDirectory, filename);
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(src);

        final File dest = generator.getDestination(schema, outputDirectory);

        final FileWriter output = new FileWriter(dest);
        boolean success = false;
        try {
            generator.generate(schema, output);
            success = true;
        } finally {
            try { output.close(); } catch(IOException ioe) { /* ignore */ }
            if(!success) {
                //noinspection ResultOfMethodCallIgnored
                dest.delete();
            }
        }
    }
}
