package org.jonasfagundes.jdbcvalet;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface QueryExecutorCommand {
  public String getSql();
  public void bind(PreparedStatement stmt) throws SQLException;
}
