package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class CountFunction extends SqlFunction<CountFunction>
{
    private boolean distinct;

    public CountFunction(Object value)
    {
        super("count");
        this.value = value.toString();
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
        String value = this.name + "(" + (this.distinct ? "DISTINCT " : "") + this.value + ")";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}