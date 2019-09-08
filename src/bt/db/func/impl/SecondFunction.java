package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class SecondFunction extends SqlFunction<SecondFunction>
{
    public SecondFunction(Object value)
    {
        super("second");

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