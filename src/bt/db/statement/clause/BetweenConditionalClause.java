package bt.db.statement.clause;

import java.sql.PreparedStatement;

import bt.db.statement.SqlStatement;

/**
 * Holds data for conditional clauses used in sql statements for BETWEEN conditions.
 * 
 * @author &#8904
 */
public class BetweenConditionalClause<T extends SqlStatement> extends ConditionalClause<T>
{
    /** The lower bound value. */
    protected String value1;

    /** The upper bound value. */
    protected String value2;

    /** The value type of the lower bound value. */
    protected ValueType valueType1;

    /** The value type of the lower bound value. */
    protected ValueType valueType2;

    /**
     * Creates a new instance.
     * 
     * @param statement
     *            The statement that creates this clause.
     * @param column
     *            The column whichs value will be checked.
     * @param value1
     *            The lower bound value.
     * @param value2
     *            The upper bound value.
     * @param valueType1
     *            The value type of the lower bound value.
     * @param valueType2
     *            The value type of the lower bound value.
     */
    public BetweenConditionalClause(T statement, String column, String value1, String value2, ValueType valueType1,
            ValueType valueType2)
    {
        super(statement, column, ConditionalClause.BETWEEN);
        this.value1 = value1;
        this.value2 = value2;
        this.valueType1 = valueType1;
        this.valueType2 = valueType2;
        this.lastParameterIndex = Integer.MIN_VALUE;
    }

    /**
     * @see bt.db.statement.clause.ConditionalClause#prepareValue(java.sql.PreparedStatement, int)
     */
    @Override
    public String prepareValue(PreparedStatement statement, int parameterIndex)
    {
        if (parameterIndex == this.lastParameterIndex + 1)
        {
            this.value = this.value2;
            this.valueType = valueType2;
        }
        else
        {
            this.value = this.value1;
            this.valueType = valueType1;
        }

        return super.prepareValue(statement, parameterIndex);
    }

    /**
     * @see bt.db.statement.clause.ConditionalClause#toString()
     */
    @Override
    public String toString()
    {
        return this.keyword + " " + this.column + " " + this.operator + " ? " + AND + " ?";
    }

    /**
     * @see bt.db.statement.clause.ConditionalClause#toString(boolean)
     */
    @Override
    public String toString(boolean prepared)
    {
        if (prepared && this.usesValue)
        {
            return toString();
        }
        
        String clause = this.keyword + " " + this.column + " " + this.operator;

        if (this.valueType1 == ValueType.DATE || this.valueType1 == ValueType.TIME
                || this.valueType1 == ValueType.TIMESTAMP)
        {
            clause += " '" + this.value1 + "'";
        }
        else
        {
            clause += " " + this.value1;
        }

        if (this.valueType2 == ValueType.DATE || this.valueType2 == ValueType.TIME
                || this.valueType2 == ValueType.TIMESTAMP)
        {
            clause += AND + " '" + this.value2 + "'";
        }
        else
        {
            clause += AND + " " + this.value2;
        }

        return clause;
    }
}