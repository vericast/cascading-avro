/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;

import java.io.File;
import java.io.IOException;

/**
 * Base for Avro Compiler Mojos.
 */
public abstract class AbstractAvroMojo extends AbstractMojo {

  /**
   * The current Maven project.
   * 
   * @parameter default-value="${project}"
   * @readonly
   * @required
   */
  private MavenProject project;

  @Override
  public void execute() throws MojoExecutionException {
    boolean hasSourceDir = null != getSourceDirectory()
        && getSourceDirectory().isDirectory();
    boolean hasTestDir = null != getTestSourceDirectory()
        && getTestSourceDirectory().isDirectory();
    if (!hasSourceDir && !hasTestDir) {
      throw new MojoExecutionException("neither sourceDirectory: "
          + getSourceDirectory() + " or testSourceDirectory: " + getTestSourceDirectory()
          + " are directories");
    }
    if (hasSourceDir) {
      String[] includedFiles = getIncludedFiles(
          getSourceDirectory().getAbsolutePath(), getExcludes(), getIncludes());
      compileFiles(includedFiles, getSourceDirectory(), getOutputDirectory());
      project.addCompileSourceRoot(getOutputDirectory().getAbsolutePath());
    }
    if (hasTestDir) {
      String[] includedFiles = getIncludedFiles(
          getTestSourceDirectory().getAbsolutePath(), getTestExcludes(),
          getTestIncludes());
      compileFiles(includedFiles, getTestSourceDirectory(), getTestOutputDirectory());
      project.addTestCompileSourceRoot(getTestOutputDirectory().getAbsolutePath());
    }
  }

    protected abstract String[] getTestExcludes();

    protected abstract String[] getExcludes();

    protected abstract File getTestOutputDirectory();

    protected abstract File getOutputDirectory();

    protected abstract File getTestSourceDirectory();

    protected abstract File getSourceDirectory();

    private String[] getIncludedFiles(String absPath, String[] excludes,
      String[] includes) {
    FileSetManager fileSetManager = new FileSetManager();
    FileSet fs = new FileSet();
    fs.setDirectory(absPath);
    fs.setFollowSymlinks(false);
    for (String include : includes) {
      fs.addInclude(include);
    }
    for (String exclude : excludes) {
      fs.addExclude(exclude);
    }
    return fileSetManager.getIncludedFiles(fs);
  }

  private void compileFiles(String[] files, File sourceDir, File outDir) throws MojoExecutionException {
    for (String filename : files) {
      try {
        doCompile(filename, sourceDir, outDir);
      } catch (IOException e) {
        throw new MojoExecutionException("Error compiling protocol file "
            + filename + " to " + outDir, e);
      }
    }
  }

  protected abstract void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException;

  protected abstract String[] getIncludes();

  protected abstract String[] getTestIncludes();

}
