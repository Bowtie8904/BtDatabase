package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 */
public class UpperFunction extends SqlFunction<UpperFunction>
{
    public UpperFunction(Object value)
    {
        super("upper");
        this.value = value.toString();
    }
}