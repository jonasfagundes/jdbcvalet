package org.jonasfagundes.jdbcvalet;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbInvoker {
  public <T> T execute(Connection connection, QueryReaderCommand<T> command) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      T result;
      long startTime;
      long endTime;

      stmt = connection.prepareStatement(command.getSql());
      command.bind(stmt);
      startTime = System.nanoTime();
      rs = stmt.executeQuery();
      endTime = System.nanoTime();
      logPerformance(startTime, endTime, command.getSql());
      result = command.parse(rs);
      rs.close();
      stmt.close();
      return result;
    } catch (SQLException e) {
      closeWithoutException(rs);
      closeWithoutException(stmt);
      rollbackWithoutException(connection);
      throw e;
    }
  }


  public void execute(Connection connection, QueryExecutorCommand command) throws SQLException {
    PreparedStatement stmt = null;

    try {
      long startTime;
      long endTime;

      stmt = connection.prepareStatement(command.getSql());
      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      stmt.close();
      logPerformance(startTime, endTime, command.getSql());
    } catch (SQLException e) {
      closeWithoutException(stmt);
      rollbackWithoutException(connection);
      throw e;
    }
  }


  public int executeWithIdentity(Connection connection, QueryExecutorCommand command) throws SQLException {
    PreparedStatement stmt = null;

    try {
      int generatedKey;
      long startTime;
      long endTime;

      stmt = connection.prepareStatement(command.getSql(), Statement.RETURN_GENERATED_KEYS);
      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      generatedKey = getGeneratedKey(stmt);
      stmt.close();
      logPerformance(startTime, endTime, command.getSql());

      return generatedKey;
    } catch (SQLException e) {
      closeWithoutException(stmt);
      rollbackWithoutException(connection);
      throw e;
    }
  }


  private int getGeneratedKey(PreparedStatement stmt) throws SQLException {
    int generatedKey;
    ResultSet rs = stmt.getGeneratedKeys();

    rs.next();
    generatedKey = rs.getInt(1);
    rs.close();

    return generatedKey;
  }


  public <T> T execute(Connection connection, StoredProcedureReaderCommand<T> command) throws SQLException {
    CallableStatement stmt = null;

    try {
      T result;
      long startTime;
      long endTime;

      stmt = connection.prepareCall(command.getSql());
      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      stmt.close();
      logPerformance(startTime, endTime, command.getSql());
      result = command.parse(stmt);
      return result;
    } catch (SQLException e) {
      closeWithoutException(stmt);
      rollbackWithoutException(connection);
      throw e;
    }
  }


  public void execute(Connection connection, StoredProcedureExecutorCommand command) throws SQLException {
    CallableStatement stmt = null;

    try {
      long startTime;
      long endTime;

      stmt = connection.prepareCall(command.getSql());
      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      stmt.close();
      logPerformance(startTime, endTime, command.getSql());
    } catch (SQLException e) {
      closeWithoutException(stmt);
      rollbackWithoutException(connection);
      throw e;

    }
  }


  private void logPerformance(long startTimeInNano, long endTimeInNano, String sql) {
    Logger logger = LoggerFactory.getLogger(getClass());

    logger.info("Took {} ms to run the query:\n{}",
                convertNanoToMilliSeconds(endTimeInNano - startTimeInNano),
                sql);
  }


  private long convertNanoToMilliSeconds(long timeInNano) {
    return timeInNano / 1000000;
  }


  private void closeWithoutException(AutoCloseable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (Exception e) {
        // Intentionally hidden, first exception will be raised
      }
    }
  }


  private void rollbackWithoutException(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        // Intentionally hidden, first exception will be raised
      }
    }
  }
}
