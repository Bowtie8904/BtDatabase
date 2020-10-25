package bt.db.exc;

/**
 * @author &#8904
 */
public class UnsupportedSqlException extends RuntimeException
{
    /**
     * Creates a new instance without a message.
     */
    public UnsupportedSqlException()
    {
        super();
    }

    /**
     * Creates a new instance with the given message.
     *
     * @param message The message to use.
     */
    public UnsupportedSqlException(String message)
    {
        super(message);
    }

    /**
     * Creates a new instance with the given message and cause.
     *
     * @param message The message to use.
     * @param cause   The error that caused this exception.
     */
    public UnsupportedSqlException(String message, Throwable cause)
    {
        super(message,
              cause);
    }
}