package org.apache.druid.segment.indexing;

import org.apache.druid.data.input.InputRow;

public interface DatasourceDemux
{
  String chooseDatasource(InputRow inputRow);
}
