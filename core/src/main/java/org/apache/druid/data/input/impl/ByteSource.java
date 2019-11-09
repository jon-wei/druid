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
import org.apache.druid.io.ByteBufferInputStream;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteSource implements ObjectSource<ByteBuffer>
{
  private final ByteBuffer buffer;

  public ByteSource(ByteBuffer buffer)
  {
    this.buffer = buffer.duplicate();
  }

  public ByteSource(byte[] bytes)
  {
    this(ByteBuffer.wrap(bytes));
  }

  @Override
  public ByteBuffer getObject()
  {
    return buffer;
  }

  @Override
  public InputStream open()
  {
    return new ByteBufferInputStream(buffer);
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}
