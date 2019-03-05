package bt.db.statement.clause.join;

import bt.db.statement.clause.JoinConditionalClause;
import bt.db.statement.impl.SelectStatement;

/**
 * Holds data for an inner join used in a SELECT statement.
 * 
 * @author &#8904
 */
public class InnerJoin extends JoinClause<InnerJoin>
{
    /** The columns used to join the tables with the USING keyword. */
    private String[] joinedColumns;

    /** Indicates whether this join uses the USING or the ON syntax. true = using, false = on. */
    private boolean using;

    /**
     * Creates a new instance and calls the super constructor to initialize fields.
     * 
     * @param statement
     *            The calling statement.
     * @param table1
     *            The first table (left side) in this join.
     * @param table2
     *            The second table (right side) of this join.
     */
    public InnerJoin(SelectStatement statement, String table1, String table2)
    {
        super(statement, table1, table2);
    }

    /**
     * Defines the columns which should be used to join.
     * 
     * <p>
     * Making use of this method means that no complex ON syntax can be used.
     * </p>
     * 
     * @param columns
     * @return
     */
    public SelectStatement using(String... columns)
    {
        this.joinedColumns = columns;
        this.using = true;
        return this.statement;
    }

    /**
     * Returns the String representing this join clause.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String sql = "";

        if (this.using)
        {
            sql = "JOIN " + this.table2 + " USING (";

            for (String column : this.joinedColumns)
            {
                sql += column + ", ";
            }

            sql = sql.substring(0, sql.length() - 2);

            sql += ")";
        }
        else
        {
            sql = "JOIN " + this.table2;

            for (JoinConditionalClause clause : this.conditionalClauses)
            {
                sql += " " + clause.toString();
            }
        }

        return sql;
    }

    /**
     * @see bt.db.statement.clause.join.JoinClause#toString(boolean)
     */
    @Override
    public String toString(boolean prepared)
    {
        String sql = "";

        if (this.using)
        {
            sql = "JOIN " + this.table2 + " USING (";

            for (String column : this.joinedColumns)
            {
                sql += column + ", ";
            }

            sql = sql.substring(0, sql.length() - 2);

            sql += ")";
        }
        else
        {
            sql = "JOIN " + this.table2;

            for (JoinConditionalClause clause : this.conditionalClauses)
            {
                sql += " " + clause.toString(prepared);
            }
        }

        return sql;
    }
}