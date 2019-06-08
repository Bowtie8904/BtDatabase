package bt.db.statement.clause.join;

import bt.db.statement.clause.ConditionalClause;
import bt.db.statement.impl.SelectStatement;

/**
 * Holds data for conditional part used in sql join on clauses.
 * 
 * @author &#8904
 */
public class JoinConditionalClause extends ConditionalClause<SelectStatement>
{
    /** The involved tables. table1 = left side, table2 = right side. */
    private String table1, table2;

    /**
     * Creates a new instance and initializes the fields.
     * 
     * @param statement
     *            The statement that created this instance.
     * @param column
     *            The name of the column that is used in this ON clause.
     * @param table1
     *            The first table involved (left side).
     * @param table2
     *            The second table involved (right side).
     * @param keyword
     *            The keyword used in this clause. Usually this would be ON.
     */
    public JoinConditionalClause(SelectStatement statement, String column, String table1, String table2,
            String keyword)
    {
        super(statement, column, keyword);
        this.table1 = table1;
        this.table2 = table2;
    }

    /**
     * @see bt.db.statement.clause.ConditionalClause#toString()
     */
    @Override
    public String toString()
    {
        if (this.operator == null)
        {
            return "Operator null";
        }

        if (this.operator.equals(IS_NOT_NULL) || this.operator.equals(IS_NULL))
        {
            return this.keyword + " " + this.table1 + "." + this.column + " " + this.operator;
        }

        if (this.valueType == ValueType.COLUMN)
        {
            return this.keyword + " " + this.table1 + "." + this.column + " " + this.operator + " " + this.table2 + "."
                    + this.value;
        }

        return this.keyword + " " + this.table1 + "." + this.column + " " + this.operator + " ?";
    }

    /**
     * @see bt.db.statement.clause.ConditionalClause#toString(boolean)
     */
    @Override
    public String toString(boolean prepared)
    {
        if (this.operator == null)
        {
            return "Operator null";
        }

        if (prepared)
        {
            return toString();
        }

        if (this.operator.equals(IS_NOT_NULL) || this.operator.equals(IS_NULL))
        {
            return this.keyword + " " + this.column + " " + this.operator;
        }
        else if (this.valueType == ValueType.COLUMN)
        {
            return this.keyword + " " + this.table1 + "." + this.column + " " + this.operator + " " + this.table2 + "."
                    + this.value;
        }
        else if (this.valueType == ValueType.STRING)
        {
            return this.keyword + " " + this.column + " " + this.operator + " '" + this.value + "'";
        }

        return this.keyword + " " + this.column + " " + this.operator + " " + this.value;
    }
}