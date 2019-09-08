package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 */
public class FloorFunction extends SqlFunction<FloorFunction>
{
    public FloorFunction(Object value)
    {
        super("floor");
        this.value = value.toString();
    }
}