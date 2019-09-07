package bt.db.statement.clause.foreign;

import bt.db.statement.clause.Column;

/**
 * Defines a column level foreign key.
 *
 * @author &#8904
 */
public class ColumnForeignKey extends ForeignKey<ColumnForeignKey>
{
    /** The column that this foreign key is for. */
    private Column column;

    public void setColumn(Column column)
    {
        this.column = column;
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