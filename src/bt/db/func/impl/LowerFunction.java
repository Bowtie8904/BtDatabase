package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 */
public class LowerFunction extends SqlFunction<LowerFunction>
{
    public LowerFunction(Object value)
    {
        super("lower");
        this.value = value.toString();
    }
}