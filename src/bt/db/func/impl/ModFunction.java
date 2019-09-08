package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * @author &#8904
 *
 */
public class ModFunction extends SqlFunction<ModFunction>
{
    private String value1;
    private String value2;

    public ModFunction(Object value1, Object value2)
    {
        super("mod");
        this.value1 = value1.toString();
        this.value2 = value2.toString();
    }

    @Override
    public String toString()
    {
        String value = this.name + "(" + this.value1 + ", " + this.value2 + ")";

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}