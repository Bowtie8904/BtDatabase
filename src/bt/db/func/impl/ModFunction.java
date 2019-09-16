package bt.db.func.impl;

import java.util.ArrayList;
import java.util.List;

import bt.db.func.SqlFunction;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class ModFunction extends SqlFunction<ModFunction>
{
    public ModFunction(Object value1, Object value2)
    {
        super("mod");
        add(value1);
        add(value2);
    }

    @Override
    public String toString()
    {
        String str = this.name + "(";

        for (Object e : this.values)
        {
            str += e.toString() + ", ";
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