package org.jonasfagundes.jdbcvalet;

import java.sql.CallableStatement;
import java.sql.SQLException;

public interface StoredProcedureReaderCommand<T> extends StoredProcedureExecutorCommand {
  public T parse(CallableStatement stmt) throws SQLException;
}
