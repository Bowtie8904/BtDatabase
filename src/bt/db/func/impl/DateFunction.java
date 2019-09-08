package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class DateFunction extends SqlFunction<DateFunction>
{
    public DateFunction(Object value)
    {
        super("date");
        this.value = value.toString();
    }
}