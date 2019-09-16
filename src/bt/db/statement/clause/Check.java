package bt.db.statement.clause;

import java.util.ArrayList;
import java.util.List;

import bt.db.statement.clause.condition.ConditionalClause;
import bt.utils.id.StringID;

/**
 * A check constraint class that can be used on column and table level.
 *
 * @author &#8904
 */
public class Check extends ConditionalClause<Check>
{
    private String name;
    private List<ConditionalClause<Check>> conditionals;

    /**
     * Creates a new instance.
     *
     * @param column
     *            The name of the column that us used in the check.
     */
    public Check(String column)
    {
        super(null, column, "");
        this.caller = this;
        this.conditionals = new ArrayList<>();
    }

    /**
     * Sets a specific name for this check constraint.
     *
     * <p>
     * If this is not called, a name will be generated.
     * </p>
     *
     * @param name
     *            The name of the check.
     * @return This instance for chaining.
     */
    public Check name(String name)
    {
        this.name = name;
        return this;
    }

    public String getName()
    {
        return this.name;
    }

    /**
     * Creates a new conditional clause to chain this check with via an AND link using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
     * @return The created SimpleConditionalClause.
     */
    public ConditionalClause<Check> and(String column)
    {
        ConditionalClause<Check> clause = new ConditionalClause<>(this,
                                                  column,
                                                  ConditionalClause.AND);

        this.conditionals.add(clause);

        return clause;
    }

    /**
     * Creates a new conditional clause to chain this check with via an OR link using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
     * @return The created SimpleConditionalClause.
     */
    public ConditionalClause<Check> or(String column)
    {
        ConditionalClause<Check> clause = new ConditionalClause<>(this,
                                                  column,
                                                  ConditionalClause.OR);

        this.conditionals.add(clause);

        return clause;
    }

    /**
     * Forms the SQL for this constraint.
     *
     * @see bt.db.statement.clause.condition.ConditionalClause#toString()
     */
    @Override
    public String toString()
    {
        if (this.name == null)
        {
            this.name = this.column + "_" + StringID.randomID(5) + "_ck";
        }

        String constraint = "CONSTRAINT " + this.name + " CHECK (" + super.toString(false).trim();

        for (var cond : this.conditionals)
        {
            constraint += " " + cond.toString(false);
        }

        constraint += ")";
        return constraint;
    }
}