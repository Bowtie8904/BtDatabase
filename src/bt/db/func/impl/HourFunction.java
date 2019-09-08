package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class HourFunction extends SqlFunction<HourFunction>
{
    public HourFunction(Object value)
    {
        super("hour");

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