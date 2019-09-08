package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class LeftTrimFunction extends SqlFunction<LeftTrimFunction>
{
    public LeftTrimFunction(Object value)
    {
        super("ltrim");
        this.value = value.toString();
    }
}