package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 */
public class AbsoluteFunction extends SqlFunction<AbsoluteFunction>
{
    private String value;

    public AbsoluteFunction(ColumnEntry column)
    {
        super("abs");
        this.value = column.toString();
    }

    public AbsoluteFunction(int value)
    {
        super("abs");
        this.value = Integer.toString(value);
    }

    public AbsoluteFunction(long value)
    {
        super("abs");
        this.value = Long.toString(value);
    }

    public AbsoluteFunction(double value)
    {
        super("abs");
        this.value = Double.toString(value);
    }

    public AbsoluteFunction(float value)
    {
        super("abs");
        this.value = Float.toString(value);
    }

    @Override
    public String toString()
    {
        String value = this.name + "(" + this.value + ")";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}