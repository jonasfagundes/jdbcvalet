package org.jonasfagundes.jdbcvalet;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbInvoker {
  private DataSource ds;
  private Logger logger;


  public DbInvoker(DataSource ds) {
    this.logger = LoggerFactory.getLogger(getClass());
    this.ds = ds;
  }


  public Connection getConnection() throws SQLException {
    return this.ds.getConnection();
  }


  public <T> T execute(Connection connection, QueryReaderCommand<T> command) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(command.getSql())) {
      T result;
      ResultSet rs;
      long startTime;
      long endTime;

      command.bind(stmt);
      startTime = System.nanoTime();
      rs = stmt.executeQuery();
      endTime = System.nanoTime();
      logPerformance(startTime, endTime, command.getSql());
      result = command.parse(rs);
      rs.close();
      return result;
    }
  }


  public <T> T execute(QueryReaderCommand<T> command) throws SQLException {
    try (Connection connection = getConnection()) {
      return execute(connection, command);
    }
  }


  public void execute(Connection connection, QueryExecutorCommand command) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(command.getSql())) {
      long startTime;
      long endTime;

      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      logPerformance(startTime, endTime, command.getSql());
    }
  }


  public void execute(QueryExecutorCommand command) throws SQLException {
    try (Connection connection = getConnection()) {
      execute(connection, command);
    }
  }


  public <T> T execute(Connection connection, StoredProcedureReaderCommand<T> command) throws SQLException {
    try (CallableStatement stmt = connection.prepareCall(command.getSql())) {
      T result;
      long startTime;
      long endTime;

      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      logPerformance(startTime, endTime, command.getSql());
      result = command.parse(stmt);
      return result;
    }
  }


  public <T> T execute(StoredProcedureReaderCommand<T> command) throws SQLException {
    try (Connection connection = getConnection()) {
      return execute(connection, command);
    }
  }


  public void execute(Connection connection, StoredProcedureExecutorCommand command) throws SQLException {
    try (CallableStatement stmt = connection.prepareCall(command.getSql())) {
      long startTime;
      long endTime;

      command.bind(stmt);
      startTime = System.nanoTime();
      stmt.execute();
      endTime = System.nanoTime();
      logPerformance(startTime, endTime, command.getSql());
    }
  }


  public void execute(StoredProcedureExecutorCommand command) throws SQLException {
    try (Connection connection = getConnection()) {
      execute(connection, command);
    }
  }


  private void logPerformance(long startTimeInNano, long endTimeInNano, String sql) {
    this.logger.info("Took {} ms to run the query:\n{}",
                     convertNanoToMilliSeconds(endTimeInNano - startTimeInNano),
                     sql);
  }


  private long convertNanoToMilliSeconds(long timeInNano) {
    return timeInNano / 1000000;
  }
}
