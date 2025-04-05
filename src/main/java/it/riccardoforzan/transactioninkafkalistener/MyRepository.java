package it.riccardoforzan.transactioninkafkalistener;

import lombok.AllArgsConstructor;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
class MyRepository {

  private final MongoOperations mongoOperations;

  public void transactionalBulkSave(List<MyDocument> documents) {
    BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MyDocument.class);
    bulkOperations.insert(documents);
    bulkOperations.execute();
  }
}
