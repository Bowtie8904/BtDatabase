package bt.db.func.impl;

import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;

/**
 * @author &#8904
 *
 */
public class CountFunction extends SqlFunction<CountFunction>
{
    private ColumnEntry column;
    private boolean distinct;

    public CountFunction(ColumnEntry column)
    {
        super("count");
        this.column = column;
    }

    /**
     * Marks the function as distinct, meaning that only unique values will be taken into account.
     * 
     * @return This function instance.
     */
    public CountFunction distinct()
    {
        this.distinct = true;
        return this;
    }

    @Override
    public String toString()
    {
        String value = this.name + "(" + (this.distinct ? "DISTINCT " : "") + this.column + ")";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}