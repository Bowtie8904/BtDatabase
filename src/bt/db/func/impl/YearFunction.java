package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class YearFunction extends SqlFunction<YearFunction>
{
    public YearFunction(Object value)
    {
        super("year");

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