package org.jonasfagundes.jdbcvalet;

import java.sql.CallableStatement;
import java.sql.SQLException;

public interface StoredProcedureExecutorCommand {
  public String getSql();
  public void bind(CallableStatement stmt) throws SQLException;
}
