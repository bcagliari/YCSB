package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;

import java.util.*;

/**
* Initialize the scenario.
* Called once, in the main client thread, before any operations are started.
*/
public class CustomWorkload extends Workload {

  private static final String TABLE_NAME = "Usuarios";

  private CounterGenerator keyGenerator;

  /**
  * Initialize the scenario.
  * Called once, in the main client thread, before any operations are started.
  */
  @Override
  public void init(Properties p) throws WorkloadException {
    // Initialization logic here
    keyGenerator = new CounterGenerator(0);
  }

  /**
  * Initialize the scenario.
  * Called once, in the main client thread, before any operations are started.
  */
  @Override
  public boolean doInsert(DB db, Object threadState) {
    // Generate a unique user ID (you might need to adjust this logic based on your requirements)
    int userId = Integer.parseInt(keyGenerator.nextValue().toString());

    // Create a record with random values (you might want to customize this part)
    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put("nome", new StringByteIterator("User" + userId));
    values.put("email", new StringByteIterator("user" + userId + "@example.com"));
    values.put("senha", new StringByteIterator("password" + userId));
    values.put("endereco", new StringByteIterator("Address" + userId));
    values.put("telefone", new StringByteIterator("123-456-7890"));

    // Insert the record into the database
    Status status = db.insert(TABLE_NAME, Integer.toString(userId), values);

    // Return true if the operation was successful
    return status == Status.OK;
  }

  /**
  * Initialize the scenario.
  * Called once, in the main client thread, before any operations are started.
  */
  @Override
  public boolean doTransaction(DB db, Object threadState) {
    // Generate a random operation (read or insert)
    String key = keyGenerator.nextValue().toString();

    Set<String> fields = null;
    Map<String, ByteIterator> result = new HashMap<>();
    Status status = db.read(TABLE_NAME, key, fields, result);

    return status == Status.OK;
  }

  /**
  * Initialize the scenario.
  * Called once, in the main client thread, before any operations are started.
  */
  @Override
  public void cleanup() throws WorkloadException {
    // Cleanup logic here
  }
}
