package bt.db.statement.clause;

import bt.db.statement.impl.SelectStatement;

/**
 * Represents an ORDER BY clause in a select statement.
 * 
 * @author &#8904
 */
public class OrderByClause
{
    /** The columns to order by. */
    private String[] columns;

    /** Indictaes whether this caluse should sort ascending or descending. true = ascending, false = descending. */
    private boolean asc;

    /** The statement that created this clause. */
    private SelectStatement statement;

    /**
     * Indictaes whether this caluse should sort null values to the top or the bottom. true = nulls at top, false =
     * nulls at bottom.
     */
    private boolean nullsFirst = false;

    /**
     * Creates a new instance and initializes fields.
     * 
     * @param statement
     *            The statement that created this clause.
     * @param columns
     *            The column names to sort by.
     */
    public OrderByClause(SelectStatement statement, String... columns)
    {
        this.columns = columns;
        this.statement = statement;
    }

    /**
     * Makes this clause sort the values ascending.
     * 
     * @return The statement that created this clause.
     */
    public SelectStatement asc()
    {
        this.asc = true;
        return this.statement;
    }

    /**
     * Makes this clause sort the values descending.
     * 
     * @return The statement that created this clause.
     */
    public SelectStatement desc()
    {
        this.asc = false;
        return this.statement;
    }

    /**
     * Makes this clause sort null values to the top.
     * 
     * @return This instance for chaining.
     */
    public OrderByClause nullsFirst()
    {
        this.nullsFirst = true;
        return this;
    }

    /**
     * Makes this clause sort null values to the bottom.
     * 
     * @return This instance for chaining.
     */
    public OrderByClause nullsLast()
    {
        this.nullsFirst = false;
        return this;
    }

    /**
     * Returns the String representing this join clause.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String sql = "ORDER BY ";

        for (String column : this.columns)
        {
            sql += column + ", ";
        }

        sql = sql.substring(0,
                            sql.length() - 2);

        sql += " " + (this.asc ? "ASC" : "DESC");

        sql += " " + (this.nullsFirst ? "NULLS FIRST" : "NULLS LAST");

        return sql;
    }
}