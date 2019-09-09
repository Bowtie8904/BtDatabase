package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class LeftPadFunction extends SqlFunction<LeftPadFunction>
{
    private int length;
    private String pad;

    public LeftPadFunction(Object value, int length, String pad)
    {
        super("lpad");
        this.value = value.toString();
        this.length = length;
        this.pad = pad;
    }

    @Override
    public String toString()
    {
        String value = this.name + "(" + this.value + ", " + this.length + ", '" + this.pad + "')";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}