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

package org.apache.druid.data.input;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.InputFormat;

import javax.annotation.Nullable;
import java.io.File;

/**
 * Abstract class for {@link InputSource}. This class provides a default implementation of {@link #reader} with
 * a sanity check. Child classes should implement one of {@link #formattableReader} or {@link #unformattableReader}
 * depending on {@link #needsFormat()}.
 */
public abstract class AbstractInputSource implements InputSource
{
  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    if (needsFormat()) {
      return formattableReader(
          inputRowSchema,
          Preconditions.checkNotNull(inputFormat, "inputFormat"),
          temporaryDirectory
      );
    } else {
      return unformattableReader(inputRowSchema, temporaryDirectory);
    }
  }

  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    throw new UnsupportedOperationException("Implement this method properly if needsFormat() = true");
  }

  protected InputSourceReader unformattableReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
  {
    throw new UnsupportedOperationException("Implement this method properly if needsFormat() = false");
  }
}
