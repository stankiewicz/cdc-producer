package com.google.dataflow.ingestion.transforms;

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ParseCDCTransform
    extends PTransform<PCollection<String>, PCollection<Row>> {

  final Class aClass;

  public ParseCDCTransform(Class aClass) {
    this.aClass = aClass;
  }

  @Override
  public PCollection<Row> expand(PCollection<String> input) {
    Schema expectedSchema = null;
    try {
      final Class<? extends Class> aClass1 = aClass.getClass();
      expectedSchema = input.getPipeline().getSchemaRegistry().getSchema(aClass);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    return input.apply("Parse JSON to Beam Rows", JsonToRow.withSchema(expectedSchema));
  }
}
