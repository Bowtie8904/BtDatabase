package bt.db.statement.clause.join;

import java.util.ArrayList;
import java.util.List;

import bt.db.statement.clause.condition.ConditionalClause;
import bt.db.statement.impl.SelectStatement;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * Base class for all join clauses.
 *
 * @author &#8904
 */
public class JoinClause implements Preparable
{
    protected static final String INNER = "INNER";
    protected static final String RIGHT = "RIGHT";
    protected static final String LEFT = "LEFT";
    protected static final String NATURAL = "NATURAL";
    protected static final String CROSS = "CROSS";

    /** The statement which created this instance. */
    protected SelectStatement statement;

    /** The involved tables. table1 = left side, table2 = right side. */
    protected String table;

    protected String tableAlias;

    /** Contains all {@link JoinConditionalClause}s which were used in this joins ON clause. */
    protected List<ConditionalClause<SelectStatement>> conditionalClauses;

    /** The type of this join. Default = INNER. */
    protected String joinType = INNER;

    /** The columns used to join the tables with the USING keyword. */
    private String[] joinedColumns;

    /** Indicates whether this join uses the USING or the ON syntax. true = using, false = on. */
    private boolean using;

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
    public JoinClause(SelectStatement statement, String table2)
    {
        this.statement = statement;
        this.table = table2;
        this.conditionalClauses = new ArrayList<>();
    }

    public JoinClause alias(String alias)
    {
        this.tableAlias = alias;
        return this;
    }

    /**
     * Returns a list of {@link ConditionClause}s that were used to create this join.
     *
     * @return The list.
     */
    public List<ConditionalClause<SelectStatement>> getConditionalClauses()
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
    public void addConditionalClause(ConditionalClause<SelectStatement> condition)
    {
        this.conditionalClauses.add(condition);
    }

    /**
     * Returns the name of the second table (right side).
     *
     * @return The name of the table.
     */
    public String getTable()
    {
        return this.table;
    }

    /**
     * Creates a new ON {@link JoinConditionalClause} which will be automatically added to this join.
     *
     * @param column
     *            The name of the column to join with.
     * @return The conditional clause.
     */
    public ConditionalClause<SelectStatement> on(String column)
    {
        ConditionalClause<SelectStatement> condition = new ConditionalClause<>(this.statement,
                                                                                              column,
                                                                                              ConditionalClause.ON);
        addConditionalClause(condition);
        this.statement.setLastConditionalType(ConditionalClause.ON);
        return condition;
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
     * Defines that this join should be treated as a natural join clause, meaning no specific columns are set and the
     * tables are joined around columns with the same name instead.
     *
     * @return
     */
    public SelectStatement natural()
    {
        this.joinType = NATURAL;
        return this.statement;
    }

    public SelectStatement cross()
    {
        this.joinType = CROSS;
        return this.statement;
    }

    /**
     * Defines that this join should be treated as a right join clause.
     *
     * @return
     */
    public JoinClause right()
    {
        this.joinType = RIGHT;
        return this;
    }

    /**
     * Defines that this join should be treated as a left join clause.
     *
     * @return
     */
    public JoinClause left()
    {
        this.joinType = LEFT;
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
        return toString(false);
    }

    /**
     * Returns the String representing this join clause.
     *
     * @param prepared
     *            Indicates whether underlying conditionals from the ON syntax should be treated as prepared statements
     *            or inserted as plain text. true = use prepared statement, false = insert plain.
     */
    public String toString(boolean prepared)
    {
        String sql = this.joinType + " JOIN " + this.table + (this.tableAlias != null ? " " + this.tableAlias : "");

        if (!this.joinType.equals(NATURAL) && !this.joinType.equals(CROSS))
        {
            if (this.using)
            {
                sql += " USING (";

                for (String column : this.joinedColumns)
                {
                    sql += column + ", ";
                }

                sql = sql.substring(0, sql.length() - 2);

                sql += ")";
            }
            else
            {
                for (var clause : this.conditionalClauses)
                {
                    sql += " " + clause.toString(prepared);
                }
            }
        }

        return sql;
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        for (var clause : this.conditionalClauses)
        {
            values.addAll(clause.getValues());
        }

        return values;
    }
}