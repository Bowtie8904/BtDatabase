package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class SumFunction extends SqlFunction<SumFunction>
{
    private String column;
    private boolean distinct;

    public SumFunction(Object value)
    {
        super("sum");
        this.column = value.toString();
    }

    /**
     * Marks the function as distinct, meaning that only unique values will be taken into account.
     *
     * @return This function instance.
     */
    public SumFunction distinct()
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