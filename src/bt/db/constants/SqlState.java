package bt.db.constants;

/**
 * @author &#8904
 *
 */
public enum SqlState
{
    /**
     * The errorcode of the exception that occours when a check constraint is violated.
     */
    CHECK_CONSTRAINT_VIOLATION("23513"),

    /**
     * The errorcode of the exception that occours when a duplicate key is inserted into a table.
     */
    DUPLICATE_KEY("23505"),

    /**
     * The errorcode of the exception that occours when a foreign key constraint is violated.
     */
    FOREIGN_KEY_VIOLATION("23503"),

    /**
     * Table '< tableName >' cannot be locked in '< mode >' mode.
     */
    CANNOT_LOCK_TABLE("X0X02"),

    /**
     * Table/View '< tableName >' does not exist.
     */
    TABLE_DOES_NOT_EXIST("X0X05"),

    /**
     * < value > '< value >' does not exist.
     */
    DOES_NOT_EXIST("X0X81"),

    /**
     * < value > '< value >' already exists.
     */
    ALREADY_EXISTS("X0Y68"),

    /**
     * < value > '< value >' already exists in < value > '< value >'.
     */
    ALREADY_EXISTS_IN("X0Y32");

    private String code;

    SqlState(String code)
    {
        this.code = code;
    }

    @Override
    public String toString()
    {
        return this.code;
    }
}