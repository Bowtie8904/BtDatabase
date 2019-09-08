package bt.db.statement.clause;

import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.func.SqlFunction;

/**
 * A clause defining the auto-generation behavior on column values.
 *
 * @author &#8904
 */
public class GenerationClause
{
    /** The generation behavior. */
    private Generated generated;

    /** The column for which this clause is used. */
    private Column column;

    /**
     * Indicates whether this columns value should be generated as identity. true = generate, false = don't generate.
     *
     * <p>
     * The behavior is highly dependent on {@link #generated} as it defines whether the value is always uniquely
     * generated or only generated if it is not inserted explicitly.
     * </p>
     */
    private boolean isIdentity;

    /** Holds the generation value for a non-identity columns. */
    private String value;

    /**
     * Creates a new instance.
     *
     * @param generated
     *            The generation behavior.
     * @param column
     *            The column that this clause is for.
     */
    public GenerationClause(Generated generated, Column column)
    {
        this.generated = generated;
        this.column = column;
    }

    /**
     * Marks this column as an identity. Identity columns values can be uniquely generated for every new row.
     *
     * <p>
     * Only BIGINT (Long) typed columns can be used as identity.
     * </p>
     *
     * <p>
     * If nothing else is specified by {@link Column#autoIncrement(int)} the value will be incremented by 1 each time.
     * </p>
     *
     * @param generated
     *            Defines the behavior of the identity as either {@link Generated#ALWAYS} or {@link Generated#DEFAULT}.
     * @return The Column for chaining.
     */
    public Column asIdentity()
    {
        if (this.column.getType() != SqlType.LONG)
        {
            throw new IllegalArgumentException("Non Integer or Long columns can't be generated as identity.");
        }

        this.isIdentity = true;
        return this.column;
    }

    /**
     * Indicates whether this clause is generating an identity column.
     *
     * @return
     */
    public boolean isIdentity()
    {
        return this.isIdentity;
    }

    /**
     * Gets the generation behavior.
     *
     * @return
     */
    public Generated getGenerationType()
    {
        return this.generated;
    }

    /**
     * Gets the generating value for non-identity columns.
     *
     * @return
     */
    public String getValueDetail()
    {
        return this.value;
    }

    /**
     * Defines the value generator.
     *
     * @param value
     * @return
     */
    public Column as(String value)
    {
        this.value = value;
        return this.column;
    }

    /**
     * Defines the value generator.
     *
     * @param func
     * @return
     */
    public Column as(SqlFunction func)
    {
        this.value = func.toString();
        return this.column;
    }

    /**
     * Forms the SQL String for this clause.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String clause = "GENERATED " + (this.generated == Generated.ALWAYS ? "ALWAYS" : "DEFAULT");

        if (this.isIdentity)
        {
            clause += " AS IDENTITY";
        }
        else
        {
            clause += " AS (" + this.value + ")";
        }

        return clause;
    }
}