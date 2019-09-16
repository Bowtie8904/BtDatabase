package bt.db.func.impl;

import java.util.ArrayList;
import java.util.List;

import bt.db.func.SqlFunction;
import bt.db.statement.value.Value;

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

        for (Object e : this.values)
        {
            str += e == null ? "NULL, " : e.toString() + ", ";
        }

        str = str.substring(0, str.length() - 2);

        str += ")";

        return str;
    }

    @Override
    public String toString(boolean prepared)
    {
        return toString();
    }

    @Override
    public List<Value> getValues()
    {
        return new ArrayList<>();
    }
}