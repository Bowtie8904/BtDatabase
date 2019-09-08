package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 */
public class AbsoluteFunction extends SqlFunction<AbsoluteFunction>
{
    public AbsoluteFunction(Object value)
    {
        super("abs");
        this.value = value.toString();
    }
}