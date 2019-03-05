package bt.db.func.impl;

import bt.db.func.SqlFunction;

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

        for (Object e : this.elements)
        {
            if (e instanceof String)
            {
                concatStr += "'" + e.toString() + "' || ";
            }
            else
            {
                concatStr += e.toString() + " || ";
            }
        }

        concatStr = concatStr.substring(0, concatStr.length() - 4);

        return concatStr;
    }
}