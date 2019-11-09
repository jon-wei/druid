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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.opencsv.CSVParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.java.util.common.parsers.Parsers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CsvReader extends TextReader
{
  private final CSVParser parser = new CSVParser();
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final Function<String, Object> multiValueFunction;
  @Nullable
  private List<String> columns;

  CsvReader(
      InputRowSchema inputRowSchema,
      String listDelimiter,
      @Nullable List<String> columns,
      boolean findColumnsFromHeader,
      int skipHeaderRows
  )
  {
    super(inputRowSchema);
    this.findColumnsFromHeader = findColumnsFromHeader;
    this.skipHeaderRows = skipHeaderRows;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));
    this.columns = findColumnsFromHeader ? null : columns; // columns will be overriden by header row

    if (this.columns != null) {
      for (String column : this.columns) {
        Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
      }
      verify(this.columns, inputRowSchema.getDimensionsSpec().getDimensionNames());
    } else {
      Preconditions.checkArgument(
          findColumnsFromHeader,
          "If columns field is not set, the first row of your data must have your header"
          + " and hasHeaderRow must be set to true."
      );
    }
  }

  @Override
  public InputRow readLine(String line) throws IOException, ParseException
  {
    final String[] parsed = parser.parseLine(line);
    final Map<String, Object> zipped = Utils.zipMapPartial(
        Preconditions.checkNotNull(columns, "columns"),
        Iterables.transform(Arrays.asList(parsed), multiValueFunction)
    );
    return MapInputRowParser.parse(
        getInputRowSchema().getTimestampSpec(),
        getInputRowSchema().getDimensionsSpec(),
        zipped
    );
  }

  @Override
  public int getNumHeaderLinesToSkip()
  {
    return skipHeaderRows;
  }

  @Override
  public boolean needsToProcessHeaderLine()
  {
    return findColumnsFromHeader;
  }

  @Override
  public void processHeaderLine(String line) throws IOException
  {
    if (!findColumnsFromHeader) {
      throw new ISE("Don't call this if findColumnsFromHeader = false");
    }
    columns = findOrCreateColumnNames(Arrays.asList(parser.parseLine(line)));
    if (columns.isEmpty()) {
      throw new ISE("Empty columns");
    }
  }

  public static void verify(List<String> columns, List<String> dimensionNames)
  {
    for (String columnName : dimensionNames) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  public static List<String> findOrCreateColumnNames(List<String> parsedLine)
  {
    final List<String> columns = new ArrayList<>(parsedLine.size());
    for (int i = 0; i < parsedLine.size(); i++) {
      if (Strings.isNullOrEmpty(parsedLine.get(i))) {
        columns.add(ParserUtils.getDefaultColumnName(i));
      } else {
        columns.add(parsedLine.get(i));
      }
    }
    if (columns.isEmpty()) {
      return ParserUtils.generateFieldNames(parsedLine.size());
    } else {
      ParserUtils.validateFields(columns);
      return columns;
    }
  }
}
