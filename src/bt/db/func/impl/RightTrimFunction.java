package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class RightTrimFunction extends SqlFunction<RightTrimFunction>
{
    public RightTrimFunction(Object value)
    {
        super("rtrim");
        this.value = value.toString();
    }
}