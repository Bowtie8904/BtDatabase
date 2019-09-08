package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class DayFunction extends SqlFunction<DayFunction>
{
    public DayFunction(Object value)
    {
        super("day");

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