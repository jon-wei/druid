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

package org.apache.druid.segment.indexing;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.List;

public class HackDatasourceDemux implements DatasourceDemux
{
  private static final EmittingLogger log = new EmittingLogger(HackDatasourceDemux.class);

  @Override
  public String chooseDatasource(InputRow inputRow)
  {
    List<String> channel = inputRow.getDimension("channel");
    if (channel == null || channel.size() != 1) {
      log.info("Demuxed datasource: Null channel");
      return "null.wikipedia";
    }

    String mychannel = channel.get(0).replace("#", "");
    log.info("Demuxed datasource: " + mychannel);
    return mychannel;
  }
}
