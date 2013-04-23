package org.jonasfagundes.jdbcvalet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class MultivaluedQuery {
  public String getPlaceholders(final Collection<? extends Object> values) {
    return getBasePlaceholders(true, values);
  }


  public String getNegatedPlaceholders(final Collection<? extends Object> values) {
    return getBasePlaceholders(false, values);
  }


  public int setValues(PreparedStatement stmt, int initialIndex, Collection<? extends Object> values) throws SQLException {
    for (Object value : values) {
      stmt.setObject(initialIndex++, value);
    }
    return initialIndex;
  }


  private String getBasePlaceholders(boolean equals, final Collection<? extends Object> values) {
    return values.size() == 1
           ? " " + getEqualityOperator(equals) + " ?"
           : " " + getIncludeOperator(equals) + " (" + StringUtils.join(getPlaceholderList(values), ", ") + ")";
  }


  private String getIncludeOperator(boolean equals) {
    return equals ? "IN" : "NOT IN";
  }


  private String getEqualityOperator(boolean equals) {
    return equals ? "=" : "<>";
  }


  private List<String> getPlaceholderList(final Collection<? extends Object> ids) {
    List<String> placeholders = new ArrayList<>();

    for (int i = ids.size(); i > 0; i--) {
      placeholders.add("?");
    }
    return placeholders;
  }
}
