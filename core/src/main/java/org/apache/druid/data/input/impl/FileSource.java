/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.ObjectSource;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

public class FileSource implements ObjectSource<File>
{
  private final File file;
  private final RandomAccessFile randomAccessFile;

  FileSource(File file) throws FileNotFoundException
  {
    this.file = file;
    this.randomAccessFile = new RandomAccessFile(file, "r");
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
  {
    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return file;
      }

      @Override
      public void close() throws IOException
      {
        // do nothing
        randomAccessFile.close();
      }
    };
  }

  @Override
  public File getObject()
  {
    return file;
  }

  @Override
  public InputStream open() throws IOException
  {
    return CompressionUtils.decompress(Channels.newInputStream(randomAccessFile.getChannel()), file.getName());
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}
