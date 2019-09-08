package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class TimeFunction extends SqlFunction<TimeFunction>
{
    public TimeFunction(Object value)
    {
        super("time");
        this.value = value.toString();
    }
}