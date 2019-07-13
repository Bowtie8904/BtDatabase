package bt.db.func;

import java.util.ArrayList;
import java.util.List;

/**
 * @author &#8904
 *
 */
public class SqlFunction<T extends SqlFunction>
{
    protected String name;
    protected String asName;
    protected List<Object> elements;

    public SqlFunction(String name)
    {
        this.name = name.toUpperCase();
        this.elements = new ArrayList<>();
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
}