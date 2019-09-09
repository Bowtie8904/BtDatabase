package bt.db.func;

import java.util.ArrayList;
import java.util.List;

/**
 * @author &#8904
 *
 */
public class SqlFunction<T extends SqlFunction>
{
    protected String value;
    protected String name;
    protected String asName;
    protected List<Object> elements;

    public SqlFunction(String name)
    {
        this.name = name.toUpperCase();
        this.elements = new ArrayList<>();
    }

    public SqlFunction(String name, Object value)
    {
        this.name = name.toUpperCase();
        this.elements = new ArrayList<>();
        this.value = value.toString();
    }

    protected void add(Object element)
    {
        this.elements.add(element);
    }

    public SqlFunction<T> as(String asName)
    {
        this.asName = asName;
        return this;
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