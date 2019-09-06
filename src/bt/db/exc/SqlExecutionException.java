package bt.db.exc;

import java.sql.SQLException;

/**
 * @author &#8904
 *
 */
public class SqlExecutionException extends SQLException
{
    private String sql;
    private SQLException cause;

    /**
     * Creates a new instance without a message.
     */
    public SqlExecutionException()
    {
        super();
    }

    /**
     * Creates a new instance with the given message.
     *
     * @param message
     *            The message to use.
     */
    public SqlExecutionException(String message)
    {
        super(message);
    }

    /**
     * Creates a new instance with the given message.
     *
     * @param message
     *            The message to use.
     * @param sql
     *            The executed sql.
     */
    public SqlExecutionException(String message, String sql)
    {
        super(message);
        this.sql = sql;
    }

    /**
     * Creates a new instance with the given message and cause.
     *
     * @param message
     *            The message to use.
     * @param cause
     *            The error that caused this exception.
     */
    public SqlExecutionException(String message, SQLException cause)
    {
        super(message);
        this.cause = cause;
    }

    /**
     * Creates a new instance with the given message and cause.
     *
     * @param message
     *            The message to use.
     * @param cause
     *            The error that caused this exception.
     * @param sql
     *            The executed sql.
     */
    public SqlExecutionException(String message, String sql, SQLException cause)
    {
        super(message);
        this.sql = sql;
        this.cause = cause;
    }

    public String getSql()
    {
        return this.sql;
    }

    @Override
    public String getSQLState()
    {
        return this.cause == null ? null : this.cause.getSQLState();
    }
}
