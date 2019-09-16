package bt.db.statement.clause;

import java.util.ArrayList;
import java.util.List;

import bt.db.statement.clause.join.JoinClause;
import bt.db.statement.impl.SelectStatement;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class FromClause implements Preparable
{
    private List<JoinClause> joins;

    private Object table;

    public FromClause(Object table)
    {
        this.table = table;
        this.joins = new ArrayList<>();
    }

    public void addJoin(JoinClause join)
    {
        this.joins.add(join);
    }

    public String getTableName()
    {
        if (this.table instanceof SelectStatement)
        {
            return ((SelectStatement)this.table).getAlias();
        }
        else
        {
            return this.table.toString();
        }
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        if (this.table instanceof SelectStatement)
        {
            values.addAll(((SelectStatement)this.table).getValues());
        }

        for (JoinClause join : this.joins)
        {
            values.addAll(join.getValues());
        }

        return values;
    }

    public String toString(boolean prepared)
    {
        String sql = "";

        if (this.table instanceof SelectStatement)
        {
            if (prepared)
            {
                ((SelectStatement)this.table).prepared();
            }
            else
            {
                ((SelectStatement)this.table).unprepared();
            }

            sql += "(" + this.table.toString() + ") " + ((SelectStatement)this.table).getAlias();
        }
        else
        {
            sql += this.table.toString();
        }

        for (JoinClause join : this.joins)
        {
            sql += " " + join.toString(prepared);
        }

        return sql;
    }

    @Override
    public String toString()
    {
        return toString(true);
    }
}