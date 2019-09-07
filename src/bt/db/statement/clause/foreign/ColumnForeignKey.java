package bt.db.statement.clause.foreign;

import bt.db.statement.clause.TableColumn;
import bt.db.statement.impl.CreateStatement;

/**
 * Defines a column level foreign key.
 *
 * @author &#8904
 */
public class ColumnForeignKey<T extends TableColumn<K>, K extends CreateStatement> extends ForeignKey<ColumnForeignKey<T, K>, K>
{
    /** The columnd that this foreign key is for. */
    private T column;

    /**
     * Creates a new instance.
     *
     * @param column
     *            The column that this foreign key is for.
     */
    public ColumnForeignKey(T column)
    {
        super(column.getStatement());
        this.column = column;
    }

    /**
     * Finishes this foreign key and returns to the calling statement.
     *
     * <p>
     * This will call {@link TableColumn#add()}.
     * </p>
     *
     * @see bt.db.statement.clause.foreign.ForeignKey#add()
     */
    @Override
    public K add()
    {
        this.column.add();
        return super.add();
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
            this.name = this.column.getStatement().getName() + "_" + this.table + "_" + this.column.getName() + "_fk";
        }

        String constraint = "CONSTRAINT " + this.name + " REFERENCES " + this.table;


        if (this.parentColumns.length > 0)
        {
            constraint += " (";

            for (String column : this.parentColumns)
            {
                constraint += column + ", ";
            }

            constraint = constraint.substring(0, constraint.length() - 2);

            constraint += ")";
        }

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