package org.jonasfagundes.jdbcvalet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public abstract class ArrayQueryReaderCommand<T> implements QueryReaderCommand<T> {
  protected String getPlaceholders(final Collection<? extends Object> values) {
    return values.size() == 1
           ? " = ?"
           : " IN (" + StringUtils.join(getPlaceholderList(values), ", ") + ")";
  }


  protected int setValues(PreparedStatement stmt, int initialIndex, Collection<? extends Object> values) throws SQLException {
    for (Object value : values) {
      stmt.setObject(initialIndex++, value);
    }
    return initialIndex;
  }

  private List<String> getPlaceholderList(final Collection<? extends Object> ids) {
    List<String> placeholders = new ArrayList<>();

    for (int i = ids.size(); i > 0; i--) {
      placeholders.add("?");
    }
    return placeholders;
  }
}
