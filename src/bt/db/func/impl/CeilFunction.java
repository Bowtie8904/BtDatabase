package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 */
public class CeilFunction extends SqlFunction<CeilFunction>
{
    public CeilFunction(Object value)
    {
        super("ceil");
        this.value = value.toString();
    }
}