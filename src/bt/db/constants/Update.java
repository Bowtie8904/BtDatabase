package bt.db.constants;

/**
 * Offers values to define foreign key on update behavior.
 *
 * @author &#8904
 */
public enum Update
{
    /**
     * Indicates that Derby should check the dependent tables for foreign key constraints after all updates have been
     * executed but before triggers have been executed. If any row in a dependent table violates a foreign key
     * constraint, the statement is rejected.
     */
    NO_ACTION("NO ACTION"),
    /**
     * Indicates that Derby should check dependent tables for foreign key constraints. If any row in a dependent table
     * violates a foreign key constraint, the transaction is rolled back.
     */
    RESTRICT("RESTRICT");

    private String name;

    Update(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return this.name;
    }
}