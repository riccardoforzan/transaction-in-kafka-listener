package it.riccardoforzan.transactioninkafkalistener;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
record MyDocument(
  @Id
  ObjectId id,
  @Indexed(unique = true)
  String value
) {
}
