package bt.db.func.impl;

import java.util.ArrayList;
import java.util.List;

import bt.db.func.SqlFunction;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class ConcatenateFunction extends SqlFunction<ConcatenateFunction>
{
    public ConcatenateFunction(Object... elements)
    {
        super("concat");
        for (Object e : elements)
        {
            add(e);
        }
    }

    @Override
    public String toString()
    {
        String concatStr = "";

        for (Object e : this.values)
        {
            concatStr += e.toString() + " || ";
        }

        concatStr = concatStr.substring(0, concatStr.length() - 4);

        return concatStr;
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