package bt.db.func.impl;

import java.util.ArrayList;
import java.util.List;

import bt.db.constants.SqlType;
import bt.db.func.Sql;
import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class RightPadFunction extends SqlFunction<RightPadFunction>
{
    private int length;
    private String pad;
    private Object value;

    public RightPadFunction(Object value, int length, String pad)
    {
        super("rpad");

        if (value instanceof String)
        {
            if (((String)value).trim().startsWith("'"))
            {
                this.value = value.toString();
            }
            else
            {
                this.value = Sql.column(value.toString());
            }
        }
        else
        {
            this.value = value;
        }

        this.length = length;
        this.pad = pad;
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    @Override
    public String toString(boolean prepared)
    {
        String value = "";

        if (prepared)
        {
            if (!(this.value instanceof ColumnEntry))
            {
                value = this.name + "(?, ?, ?)";
            }
            else
            {
                value = this.name + "(" + this.value + ", ?, ?)";
            }
        }
        else
        {
            value = this.name + "(" + this.value + ", " + this.length + ", '" + this.pad + "')";
        }

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }

    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        if (!(this.value instanceof ColumnEntry))
        {
            if (((String)this.value).trim().startsWith("'"))
            {
                values.add(new Value(SqlType.VARCHAR, this.value.toString().substring(1, this.value.toString().length() - 1)));
            }
            else
            {
                values.add(new Value(SqlType.VARCHAR, this.value));
            }
        }

        values.add(new Value(SqlType.INTEGER, this.length));
        values.add(new Value(SqlType.VARCHAR, this.pad));

        return values;
    }
}