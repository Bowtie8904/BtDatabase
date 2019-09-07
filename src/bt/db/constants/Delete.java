package bt.db.constants;

/**
 * Offers values to define foreign key on delete behavior.
 *
 * @author &#8904
 */
public enum Delete
{
    /**
     * Indicates that Derby should check the dependent tables for foreign key constraints after all deletes have been
     * executed but before triggers have been executed. If any row in a dependent table violates a foreign key
     * constraint, the statement is rejected.
     */
    NO_ACTION("NO ACTION"),
    /**
     * Indicates that each nullable column of the dependent table's foreign key is set to null. (if the dependent table
     * also has dependent tables, nullable columns in those tables' foreign keys are also set to null.)
     */
    SET_NULL("SET NULL"),
    /**
     * Indicates that the delete operation is propagated to the dependent table (and that table's dependents, if
     * applicable).
     */
    CASCADE("CASCADE"),
    /**
     * Indicates that Derby should check dependent tables for foreign key constraints. If any row in a dependent table
     * violates a foreign key constraint, the transaction is rolled back.
     */
    RESTRICT("RESTRICT");

    private String name;

    Delete(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return this.name;
    }
}