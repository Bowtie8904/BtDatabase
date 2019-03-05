package bt.db.statement.clause;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import bt.db.statement.impl.CreateTriggerStatement;

/**
 * Defines the action that is taken when the trigger, that created this instance, is fired.
 * 
 * @author &#8904
 */
public class TriggerAction
{
    /** The statement that created this instance. */
    private CreateTriggerStatement statement;

    /** The name of the sql procedure tht should be called when the trigger fires. */
    private String procedure;

    private String sql;

    /**
     * The paramaters that will be used to call the defined procedure. Values of types that require quotation marks will
     * be treated correctly.
     */
    private String[] parameters;

    /**
     * Creates a new instance and initializes the fields.
     * 
     * @param statement
     *            The statement that created this instance.
     */
    public TriggerAction(CreateTriggerStatement statement)
    {
        this.statement = statement;
        this.parameters = new String[] {};
    }

    /**
     * Defines an sql statement which will be executed on activation of this trigger.
     * 
     * @param sql
     *            The raw sql statement.
     * @return The statement which created this instance.
     */
    public CreateTriggerStatement execute(String sql)
    {
        this.sql = sql;
        return this.statement;
    }

    /**
     * Defines the sql procedure that should be called when the trigger fires.
     * 
     * @param procedure
     *            The name of the sql procedure.
     * @return This instance for chaining.
     */
    public TriggerAction call(String procedure)
    {
        this.procedure = procedure;
        return this;
    }

    /**
     * Defines the paramaters that are used to call the procedure with.
     * 
     * <p>
     * Quotation marks will automatically be added around types like String, Date and Time.
     * </p>
     * 
     * <p>
     * Use {@link ColumnEntry} objects to specify that the value of a table column should be used instead of the given
     * String value.
     * </p>
     * 
     * @param parameters
     *            An array of parameters. The toString() implementation of the given object will be used.
     * @return The statement that created this instance.
     */
    public CreateTriggerStatement with(Object... parameters)
    {
        this.parameters = new String[parameters.length];

        for (int i = 0; i < parameters.length; i ++ )
        {
            Object obj = parameters[i];

            if (obj instanceof String || obj instanceof Date || obj instanceof Time || obj instanceof Timestamp)
            {
                this.parameters[i] = "'" + obj.toString() + "'";
            }
            else
            {
                this.parameters[i] = obj.toString();
            }
        }

        return this.statement;
    }

    /**
     * Returns the String representing this trigger action.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String action = "";

        if (this.procedure != null)
        {
            action += "CALL " + this.procedure + "(";

            for (String param : this.parameters)
            {
                action += param + ", ";
            }

            action = action.substring(0, action.length() - 2);

            action += ")";
        }
        else if (this.sql != null)
        {
            action = this.sql;
        }

        return action;
    }
}