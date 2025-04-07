package it.riccardoforzan.transactioninkafkalistener;

import com.mongodb.bulk.BulkWriteInsert;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
@Slf4j
class MyService {

  private final MyRepository myRepository;

  @Transactional(transactionManager = "mongoTransactionManager")
  public void transactionalBulkSave(List<MyDocument> documents) {
    try {
      myRepository.transactionalBulkSave(documents);
    } catch (BulkOperationException e) {
      log.error("BulkOperationException", e);
      Set<String> collect = e
                              .getResult()
                              .getInserts()
                              .stream()
                              .map(this::getInsertedId)
                              .map(String::valueOf)
                              .collect(Collectors.toSet());
      List<MyDocument> valid = documents
                                 .stream()
                                 .filter(doc -> collect.contains(doc
                                                                   .id()
                                                                   .toString()))
                                 .toList();
      log.info("saved {}", valid);
    } catch (Exception e) {
      log.error("Exception in MyService", e);
    }
  }

  private ObjectId getInsertedId(BulkWriteInsert insert) {
    return insert.getId().asObjectId().getValue();
  }
}