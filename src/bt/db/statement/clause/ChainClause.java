package bt.db.statement.clause;

import java.util.List;

import bt.db.statement.impl.SelectStatement;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * Combines two SelectStatement with a chaining keyword such as UNION, INTERSECT or EXCEPT.
 *
 * @author &#8904
 */
public class ChainClause implements Preparable
{
    /** The standard UNION keyword. Causing only unique rows in the resultset. */
    public static final String UNION = "UNION";
    /** The UNION ALL keyword. Causing the resultset to contain all combined rows. */
    public static final String UNION_ALL = "UNION ALL";
    /** The standard INTERSECT keyword. Causing only unique rows in the resultset. */
    public static final String INTERSECT = "INTERSECT";
    /** The INTERSECT ALL keyword. Causing the resultset to contain all combined rows. */
    public static final String INTERSECT_ALL = "INTERSECT ALL";
    /** The standard INTERSECT keyword. Causing only unique rows in the resultset. */
    public static final String EXCEPT = "EXCEPT";
    /** The INTERSECT ALL keyword. Causing the resultset to contain all combined rows. */
    public static final String EXCEPT_ALL = "EXCEPT ALL";

    /** The chain keyword used. */
    private String keyword;

    /** The appended statement. */
    private SelectStatement statement;

    /**
     * Creates a new instance and sets its fields.
     *
     * @param keyword
     *            Either {@link #UNION} or {@link #UNION_ALL}.
     * @param statement
     *            The SelectStatement to append.
     */
    public ChainClause(String keyword, SelectStatement statement)
    {
        this.keyword = keyword;
        this.statement = statement;
    }

    /**
     * Creates a formatted chain clause String. The statement will be set to unprepared and appended as String.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return toString(true);
    }

    public String toString(boolean prepared)
    {
        String select = prepared ? this.statement.prepared().toString() : this.statement.unprepared().toString();
        return this.keyword + " " + select;
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        return this.statement.getValues();
    }
}