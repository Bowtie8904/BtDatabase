package bt.db.statement.value;

import bt.db.constants.SqlType;

/**
 * @author &#8904
 *
 */
public class Value
{
    private SqlType type;
    private Object value;

    public Value(SqlType type, Object value)
    {
        this.type = type;
        this.value = value;
    }

    /**
     * @return the type
     */
    public SqlType getType()
    {
        return this.type;
    }

    /**
     * @return the value
     */
    public Object getValue()
    {
        return this.value;
    }
}