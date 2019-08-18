package bt.db.statement.clause;

import bt.db.statement.impl.SelectStatement;

/**
 * Combines a union keyword and a SelectStatement.
 *
 * @author &#8904
 */
public class UnionClause
{
    /** The standard UNION keyword. Causing only unique rows in the resultset. */
    public static final String UNION = "UNION";
    /** The UNION ALL keyword. Causing the resultset to contain all combined rows. */
    public static final String UNION_ALL = "UNION ALL";

    /** The union keyword used. */
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
    public UnionClause(String keyword, SelectStatement statement)
    {
        this.keyword = keyword;
        this.statement = statement;
    }

    /**
     * Creates a formatted UNION clause String. The statement will be set to unprepared and appended as String.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return this.keyword + " " + this.statement.unprepared().toString();
    }
}