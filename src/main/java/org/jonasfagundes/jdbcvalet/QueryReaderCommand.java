package org.jonasfagundes.jdbcvalet;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface QueryReaderCommand<T> extends QueryExecutorCommand {
  public T parse(ResultSet rs) throws SQLException;
}
