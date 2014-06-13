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
  private Logger logger;


  public DbInvoker() {
    this(LoggerFactory.getLogger(DbInvoker.class));
  }


  public DbInvoker(Logger logger) {
    this.logger = logger;
  }


  public <T> T execute(Connection connection, QueryReaderCommand<T> command) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(command.getSql());
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
    stmt.close();
    return result;
  }


  public void execute(Connection connection, QueryExecutorCommand command) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(command.getSql());
    long startTime;
    long endTime;

    command.bind(stmt);
    startTime = System.nanoTime();
    stmt.execute();
    endTime = System.nanoTime();
    stmt.close();
    logPerformance(startTime, endTime, command.getSql());
  }


  public int executeWithIdentity(Connection connection, QueryExecutorCommand command) throws SQLException {
    int generatedKey;
    PreparedStatement stmt = connection.prepareStatement(command.getSql(), Statement.RETURN_GENERATED_KEYS);
    long startTime;
    long endTime;

    command.bind(stmt);
    startTime = System.nanoTime();
    stmt.execute();
    endTime = System.nanoTime();
    generatedKey = getGeneratedKey(stmt);
    stmt.close();
    logPerformance(startTime, endTime, command.getSql());

    return generatedKey;
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
    CallableStatement stmt = connection.prepareCall(command.getSql());
    T result;
    long startTime;
    long endTime;

    command.bind(stmt);
    startTime = System.nanoTime();
    stmt.execute();
    endTime = System.nanoTime();
    stmt.close();
    logPerformance(startTime, endTime, command.getSql());
    result = command.parse(stmt);
    return result;
  }


  public void execute(Connection connection, StoredProcedureExecutorCommand command) throws SQLException {
    CallableStatement stmt = connection.prepareCall(command.getSql());
    long startTime;
    long endTime;

    command.bind(stmt);
    startTime = System.nanoTime();
    stmt.execute();
    endTime = System.nanoTime();
    stmt.close();
    logPerformance(startTime, endTime, command.getSql());
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
