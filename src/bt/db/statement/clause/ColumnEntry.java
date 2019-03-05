package bt.db.statement.clause;

/**
 * Represents a table.column combination. An instance of this class should be used as parameter for sql clauses so that
 * the parameter is not mistaken for a String literal and is therefore not put into quotations.
 * 
 * <p>
 * A simple way to initialize this class would be <br>
 * <code>Sql.column("tableName", "columnName");</code>
 * </p>
 * 
 * @author &#8904
 */
public class ColumnEntry
{
    private String tableName;
    private String columnName;
    private String asName;

    /**
     * Creates a new instance.
     * 
     * @param table
     *            The name of the table.
     * @param collumn
     *            The name of the column.
     */
    public ColumnEntry(String table, String column)
    {
        this.tableName = table;
        this.columnName = column;
    }

    /**
     * Creates a new instance.
     * 
     * @param collumn
     *            The name of the column.
     */
    public ColumnEntry(String column)
    {
        this.columnName = column;
    }

    /**
     * Defines an alias for this combination.
     * 
     * <p>
     * This will simple append <br>
     * <code>AS asName</code> <br>
     * at the end of the created sql String from toString().
     * </p>
     * 
     * @param asName
     *            The alias.
     * @return This isntance.
     */
    public ColumnEntry as(String asName)
    {
        this.asName = asName;
        return this;
    }

    /**
     * Returns a valid sql String representation.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String value = (this.tableName != null) ? (this.tableName + "." + this.columnName) : this.columnName;

        if (this.asName != null)
        {
            value += " AS " + this.asName;
        }

        return value;
    }
}