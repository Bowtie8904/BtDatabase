package bt.db.statement.clause.join;

import java.util.ArrayList;
import java.util.List;

import bt.db.statement.clause.ConditionalClause;
import bt.db.statement.clause.JoinConditionalClause;
import bt.db.statement.impl.SelectStatement;

/**
 * Base class for all join clauses.
 * 
 * @author &#8904
 */
public abstract class JoinClause<T extends JoinClause>
{
    /** The statement which created this instance. */
    protected SelectStatement statement;

    /** The involved tables. table1 = left side, table2 = right side. */
    protected String table1, table2;

    /** Contains all {@link JoinConditionalClause}s which were used to create this join. */
    protected List<JoinConditionalClause> conditionalClauses;

    /**
     * Creates a new instance and initializes fields.
     * 
     * @param statement
     *            The calling statement.
     * @param table1
     *            The first table (left side) in this join.
     * @param table2
     *            The second table (right side) of this join.
     */
    public JoinClause(SelectStatement statement, String table1, String table2)
    {
        this.statement = statement;
        this.table1 = table1;
        this.table2 = table2;
        this.conditionalClauses = new ArrayList<>();
    }

    /**
     * Returns a list of {@link JoinConditionalClause}s that were used to create this join.
     * 
     * @return The list.
     */
    public List<JoinConditionalClause> getConditionalClauses()
    {
        return this.conditionalClauses;
    }

    /**
     * Adds the given conditional clause to this join.
     * 
     * <p>
     * This method should never be used explicitly. Creating a conditional through the {@link #on(String)} method will
     * automatically add the clause to this join.
     * </p>
     * 
     * @param condition
     */
    public void addConditionalClause(JoinConditionalClause condition)
    {
        this.conditionalClauses.add(condition);
    }

    /**
     * Returns the name of the first table (left side).
     * 
     * @return The name of the table.
     */
    public String getFirstTable()
    {
        return this.table1;
    }

    /**
     * Returns the name of the second table (right side).
     * 
     * @return The name of the table.
     */
    public String getSecondTable()
    {
        return this.table2;
    }

    /**
     * Creates a new ON {@link JoinConditionalClause} which will be automatically added to this join.
     * 
     * @param column
     *            The name of the column to join with.
     * @return The conditional clause.
     */
    public JoinConditionalClause on(String column)
    {
        JoinConditionalClause condition = new JoinConditionalClause(this.statement, column, this.table1, this.table2,
                ConditionalClause.ON);
        addConditionalClause(condition);
        this.statement.setLastConditionalType(ConditionalClause.ON);
        return condition;
    }

    /**
     * Returns the String representing this join clause.
     * 
     * @param prepared
     *            Indicates whether underlying conditionals from the ON syntax should be treated as prepared statements
     *            or inserted as plain text. true = use prepared statement, false = insert plain.
     */
    public abstract String toString(boolean prepared);
}