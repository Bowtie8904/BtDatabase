package bt.db.func;

import java.util.ArrayList;
import java.util.List;

import bt.db.constants.SqlType;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class SqlFunction<T extends SqlFunction> implements Preparable
{
    protected String name;
    protected String asName;
    protected List<Object> values;
    protected boolean distinct;

    public SqlFunction(String name)
    {
        this.name = name.toUpperCase();
        this.values = new ArrayList<>();
    }

    public SqlFunction(String name, Object value)
    {
        this.name = name.toUpperCase();
        this.values = new ArrayList<>();
        add(value);
    }

    protected void add(Object element)
    {
        if (element instanceof String)
        {
            if (((String)element).trim().startsWith("'"))
            {
                this.values.add(element);
            }
            else
            {
                this.values.add(Sql.column(element.toString()));
            }
        }
        else
        {
            this.values.add(element);
        }
    }

    public SqlFunction<T> as(String asName)
    {
        this.asName = asName;
        return this;
    }

    public SqlFunction<T> distinct()
    {
        this.distinct = true;
        return this;
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean prepared)
    {
        String value = this.name + "(" + (this.distinct ? "DISTINCT " : "");

        for (Object obj : this.values)
        {
            value += prepared ? "?, " : obj.toString() + ", ";
        }

        value = value.substring(0, value.length() - 2);

        value += ")";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        for (Object e : this.values)
        {
            if (!(e instanceof ColumnEntry))
            {
                values.add(new Value(SqlType.convert(e.getClass()), e));
            }
        }

        return values;
    }
}