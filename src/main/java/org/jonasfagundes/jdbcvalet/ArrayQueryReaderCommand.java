package org.jonasfagundes.jdbcvalet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public abstract class ArrayQueryReaderCommand<T> implements QueryReaderCommand<T> {
  protected String getIdsPlaceholders(final Collection<Integer> ids) {
    return ids.size() == 1
         ? " = ?"
         : " IN (" + StringUtils.join(getPlaceholderList(ids), ", ") + ")";
  }


  protected int setIds(PreparedStatement stmt, int initialIndex, Collection<Integer> ids) throws SQLException {
    for (int id : ids) {
      stmt.setInt(initialIndex++, id);
    }
    return initialIndex;
  }

  private List<String> getPlaceholderList(final Collection<Integer> ids) {
    List<String> placeholders = new ArrayList<>();

    for (int i = ids.size(); i > 0; i--) {
      placeholders.add("?");
    }
    return placeholders;
  }
}
