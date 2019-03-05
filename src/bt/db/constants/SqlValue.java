package bt.db.constants;

/**
 * Defines database specific value keywords.
 * 
 * @author &#8904
 */
public enum SqlValue
{
    /**
     * CURRENT_TIMESTAMP returns the current timestamp; the value returned does not change if it is executed more than
     * once in a single statement. This means the value is fixed even if there is a long delay between fetching rows in
     * a cursor.
     */
    CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"),

    /**
     * CURRENT_TIME returns the current time; the value returned does not change if it is executed more than once in a
     * single statement. This means the value is fixed even if there is a long delay between fetching rows in a cursor.
     */
    CURRENT_TIME("CURRENT_TIME"),

    /**
     * CURRENT_DATE returns the current date; the value returned does not change if it is executed more than once in a
     * single statement. This means the value is fixed even if there is a long delay between fetching rows in a cursor.
     */
    CURRENT_DATE("CURRENT_DATE"),

    /**
     * SYSTIMESTAMP is a synonym for CURRENT_TIMESTAMP and returns the current timestamp; the value returned does not
     * change if it is executed more than once in a single statement. This means the value is fixed even if there is a
     * long delay between fetching rows in a cursor.
     */
    SYSTIMESTAMP("CURRENT_TIMESTAMP"),

    /**
     * SYSTIME is a synonym for CURRENT_TIME and returns the current time; the value returned does not change if it is
     * executed more than once in a single statement. This means the value is fixed even if there is a long delay
     * between fetching rows in a cursor.
     */
    SYSTIME("CURRENT_TIME"),

    /**
     * SYSDATE is a synonym for CURRENT_DATE and returns the current date; the value returned does not change if it is
     * executed more than once in a single statement. This means the value is fixed even if there is a long delay
     * between fetching rows in a cursor.
     */
    SYSDATE("CURRENT_DATE");

    /**
     * The name of the table that contains an entry for every table currently in use on this database.
     */
    public static final String SYSTABLE = "SYS.SYSTABLES";
    private String literal;

    /**
     * @param literal
     *            The database keyword that will be inserted into statements.
     */
    SqlValue(String literal)
    {
        this.literal = literal;
    }

    @Override
    public String toString()
    {
        return this.literal;
    }
}