package bt.db.exc;

/**
 * Thrown when an error during automated initialization or persistance of objects occurred.
 * 
 * @author &#8904
 */
public class SqlEntryException extends RuntimeException
{
    /**
     * Creates a new instance without a message.
     */
    public SqlEntryException()
    {
        super();
    }

    /**
     * Creates a new instance with the given message.
     * 
     * @param message
     *            The message to use.
     */
    public SqlEntryException(String message)
    {
        super(message);
    }

    /**
     * Creates a new instance with the given message and cause.
     * 
     * @param message
     *            The message to use.
     * @param cause
     *            The error that caused this exception.
     */
    public SqlEntryException(String message, Throwable cause)
    {
        super(message, cause);
    }
}