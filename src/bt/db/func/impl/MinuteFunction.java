package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class MinuteFunction extends SqlFunction<MinuteFunction>
{
    public MinuteFunction(Object value)
    {
        super("minute");

        if (value instanceof ColumnEntry)
        {
            this.value = value.toString();
        }
        else
        {
            this.value = "'" + value.toString() + "'";
        }
    }
}