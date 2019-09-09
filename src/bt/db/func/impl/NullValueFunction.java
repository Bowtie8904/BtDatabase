package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class NullValueFunction extends SqlFunction<NullValueFunction>
{
    public NullValueFunction(Object value1, Object value2, Object... elements)
    {
        super("coalesce");

        add(value1);
        add(value2);

        for (Object e : elements)
        {
            add(e);
        }
    }

    @Override
    public String toString()
    {
        String str = this.name + "(";

        for (Object e : this.elements)
        {
            str += e == null ? "NULL, " : e.toString() + ", ";
        }

        str = str.substring(0, str.length() - 2);

        str += ")";

        return str;
    }
}