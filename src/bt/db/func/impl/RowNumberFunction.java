package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class RowNumberFunction extends SqlFunction<RowNumberFunction>
{
    public RowNumberFunction()
    {
        super("row_number() over ()");
    }

    @Override
    public String toString()
    {
        String value = this.name;

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}