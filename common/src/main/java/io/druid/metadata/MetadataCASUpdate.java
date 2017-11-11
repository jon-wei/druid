package io.druid.metadata;

/**
 * Expresses a single compare-and-swap update for MetadataStorageConnector's compareAndSwap method
 */
public class MetadataCASUpdate
{
  private final String tableName;
  private final String keyColumn;
  private final String valueColumn;
  private final String key;
  private final byte[] oldValue;
  private final byte[] newValue;

  public MetadataCASUpdate(
      String tableName,
      String keyColumn,
      String valueColumn,
      String key,
      byte[] oldValue,
      byte[] newValue
  )
  {
    this.tableName = tableName;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  public String getTableName()
  {
    return tableName;
  }

  public String getKeyColumn()
  {
    return keyColumn;
  }

  public String getValueColumn()
  {
    return valueColumn;
  }

  public String getKey()
  {
    return key;
  }

  public byte[] getOldValue()
  {
    return oldValue;
  }

  public byte[] getNewValue()
  {
    return newValue;
  }
}
