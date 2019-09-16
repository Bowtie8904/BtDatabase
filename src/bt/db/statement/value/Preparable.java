package bt.db.statement.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author &#8904
 *
 */
public interface Preparable
{
    public List<Value> getValues();

    public static void prepareStatement(PreparedStatement statement, List<Value> values) throws SQLException
    {
        prepareStatement(statement, values, 0);
    }

    public static void prepareStatement(PreparedStatement statement, List<Value> values, int startIndex) throws SQLException
    {
        Value value = null;

        for (int i = 0; i < values.size(); i ++ )
        {
            value = values.get(i);

            switch (value.getType())
            {
                case DATE:
                    statement.setDate(startIndex + i + 1, Date.valueOf(value.getValue().toString()));
                    break;
                case TIME:
                    statement.setTime(startIndex + i + 1, Time.valueOf(value.getValue().toString()));
                    break;
                case TIMESTAMP:
                    statement.setTimestamp(startIndex + i + 1, Timestamp.valueOf(value.getValue().toString()));
                    break;
                case INTEGER:
                    statement.setInt(startIndex + i + 1, Integer.parseInt(value.getValue().toString()));
                    break;
                case LONG:
                    statement.setLong(startIndex + i + 1, Long.parseLong(value.getValue().toString()));
                    break;
                case DOUBLE:
                    statement.setDouble(startIndex + i + 1, Double.parseDouble(value.getValue().toString()));
                    break;
                case FLOAT:
                    statement.setFloat(startIndex + i + 1, Float.parseFloat(value.getValue().toString()));
                    break;
                case BOOLEAN:
                    statement.setBoolean(startIndex + i + 1, Boolean.parseBoolean(value.getValue().toString()));
                    break;
                default:
                    statement.setString(startIndex + i + 1, value.getValue().toString());

            }
        }
    }
}