package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class LengthFunction extends SqlFunction<LengthFunction>
{
    public LengthFunction(Object value)
    {
        super("length");
        this.value = value.toString();
    }
}