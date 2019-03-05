package bt.db.statement.clause.join;

import bt.db.statement.clause.JoinConditionalClause;
import bt.db.statement.impl.SelectStatement;

/**
 * Holds data for an outer join used in a SELECT statement.
 * 
 * @author &#8904
 */
public class OuterJoin extends JoinClause<InnerJoin>
{
    /** Indicates whether this is a left or a right join. true = left, false = right. */
    private boolean left;

    /**
     * Creates a new instance and calls the super constructor to initialize fields.
     * 
     * @param statement
     *            The calling statement.
     * @param table1
     *            The first table (left side) in this join.
     * @param table2
     *            The second table (right side) of this join.
     * @param leftJoin
     *            true if this should be a left join, false if this should be a right join.
     */
    public OuterJoin(SelectStatement statement, String table1, String table2, boolean leftJoin)
    {
        super(statement, table1, table2);
        this.left = leftJoin;
    }

    /**
     * Returns the String representing this join clause.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.left ? "LEFT " : "RIGHT ";
        sql += "JOIN " + this.table2;

        for (JoinConditionalClause clause : this.conditionalClauses)
        {
            sql += " " + clause.toString();
        }

        return sql;
    }

    /**
     * @see bt.db.statement.clause.join.JoinClause#toString(boolean)
     */
    @Override
    public String toString(boolean prepared)
    {
        String sql = this.left ? "LEFT " : "RIGHT ";
        sql += "JOIN " + this.table2;

        for (JoinConditionalClause clause : this.conditionalClauses)
        {
            sql += " " + clause.toString(prepared);
        }

        return sql;
    }
}
