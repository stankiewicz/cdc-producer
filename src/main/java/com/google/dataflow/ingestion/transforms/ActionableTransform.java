package com.google.dataflow.ingestion.transforms;

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ActionableTransform extends PTransform<PCollection<Row>, PCollection<Row>> {


  private final Map<String,String> filters;
  private final String primaryKey;

  public ActionableTransform(String primaryKey, Map<String,String> filters) {
    this.filters = filters;
    this.primaryKey = primaryKey;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    StringBuilder sb = new StringBuilder();
    StringBuilder where = new StringBuilder();

    sb.append("select ");
    for(Map.Entry<String, String> entry : filters.entrySet()){
      sb.append("("+entry.getValue()+") as ");
      sb.append(entry.getKey());
      sb.append(",");
      where.append("(");
      where.append(entry.getValue());
      where.append(") or ");

    }
    where.append("FALSE");
    sb.append("CAST(`"+primaryKey+"` as varchar(255)) as key");// cast??
    sb.append(" from PCOLLECTION WHERE ");
    sb.append(where.toString());
    return input.apply("Filter actionable",
        SqlTransform.query(sb.toString()));
  }
}
