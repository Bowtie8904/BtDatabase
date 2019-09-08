package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class MinFunction extends SqlFunction<MinFunction>
{
    private boolean distinct;

    public MinFunction(Object value)
    {
        super("min");
        this.value = value.toString();
    }

    /**
     * Marks the function as distinct, meaning that only unique values will be taken into account.
     *
     * @return This function instance.
     */
    public MinFunction distinct()
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