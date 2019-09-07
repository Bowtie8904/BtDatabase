package bt.db.statement.clause.foreign;

import bt.db.statement.impl.CreateStatement;
import bt.utils.id.StringID;

/**
 * Defines a table level foreign key.
 *
 * @author &#8904
 */
public class TableForeignKey<K extends CreateStatement> extends ForeignKey<TableForeignKey<K>, K>
{
    /**
     * Creates a new instance.
     *
     * @param statement
     *            The calling create statement.
     * @param childColumns
     *            The columns that are used in the table that the foreign key is created in.
     */
    public TableForeignKey(K statement, String[] childColumns)
    {
        super(statement, childColumns);
    }

    /**
     * Forms the constraint SQL string.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        if (this.name == null)
        {
            this.name = this.statement.getName() + "_" + this.table + "_" + StringID.randomID(5) + "_fk";
        }

        String constraint = "CONSTRAINT " + this.name + " FOREIGN KEY (";

        if (this.childColumns.length > 0)
        {

            for (String column : this.childColumns)
            {
                constraint += column + ", ";
            }

            constraint = constraint.substring(0, constraint.length() - 2);

        }

        constraint += ") REFERENCES " + this.table + " (";

        if (this.parentColumns.length > 0)
        {
            for (String column : this.parentColumns)
            {
                constraint += column + ", ";
            }

            constraint = constraint.substring(0, constraint.length() - 2);

        }

        constraint += ")";

        if (this.onDelete != null)
        {
            constraint += " ON DELETE " + this.onDelete.toString();
        }

        if (this.onUpdate != null)
        {
            constraint += " ON UPDATE " + this.onUpdate.toString();
        }

        return constraint;
    }
}