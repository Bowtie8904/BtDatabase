package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.clause.Column;

/**
 * @author &#8904
 *
 */
public class CreateTemporaryTableStatement extends CreateStatement<CreateTemporaryTableStatement, CreateTemporaryTableStatement>
{
    /** The columns that will be added to the new table. */
    private List<Column> tableColumns;

    /** The select to create a copy table of. */
    private SelectStatement asCopySelect;

    /** Indicates whether a table copy should be created with data. */
    private boolean copyData = true;

    private boolean preserve;

    /**
     * Creates a new instance.
     *
     * @param db
     *            The database that should be used for the statement.
     * @param name
     *            The name of the table that should be created.
     */
    public CreateTemporaryTableStatement(DatabaseAccess db, String name)
    {
        super(db,
              name);
        this.statementKeyword = "DECLARE GLOBAL TEMPORARY TABLE";
        this.tableColumns = new ArrayList<>();
    }

    /**
     * Creates a new column in this table which has the given name and the given sql type.
     *
     * @param name
     *            The name of the column.
     * @param type
     *            The {@link SqlType type} of the column.
     * @return The created column.
     */
    public CreateTemporaryTableStatement column(Column column)
    {
        this.tableColumns.add(column);
        column.setStatement(this);
        return this;
    }

    /**
     * Creates this table as a copy of the given select.
     *
     * <p>
     * By default the new table will contain the data from the given select. Call {@link #withData(boolean)} to create
     * the table without data.
     * </p>
     *
     * <p>
     * Note that the select will be executed unprepared and that no default triggers will be created for the copied
     * table.
     * </p>
     *
     * @param select
     * @return
     */
    public CreateTemporaryTableStatement as(SelectStatement select)
    {
        this.asCopySelect = select.unprepared();
        return this;
    }

    /**
     * Creates a copy of the given table.
     *
     * <p>
     * By default the new table will contain the data from the given one. Call {@link #withData(boolean)} to create the
     * table without data.
     * </p>
     *
     * <p>
     * Note that no default triggers will be created for the copied table.
     * </p>
     *
     * @param table
     * @return
     */
    public CreateTemporaryTableStatement asCopyOf(String table)
    {
        this.asCopySelect = this.db.select()
                                   .from(table)
                                   .unprepared()
                                   .onLessThan(1,
                                               (i, set) ->
                                               {
                                                   return set;
                                               });
        return this;
    }

    /**
     * Indictaes whether this table should be filled with data of the copied table/select.
     *
     * <p>
     * This method has no effect unless you use {@link #as(SelectStatement)} or {@link #asCopyOf(String)}.
     * </p>
     *
     * <p>
     * The default setting is that all data will be copied into the new table.
     * </p>
     *
     * @param copyData
     * @return
     */
    public CreateTemporaryTableStatement withData(boolean copyData)
    {
        this.copyData = copyData;
        return this;
    }

    @Override
    public CreateTemporaryTableStatement replace()
    {
        throw new UnsupportedOperationException("Can't replace temporary tables.");
    }

    /**
     * Indicates that the rows in this table should be preserved on commit.
     *
     * @return
     */
    public CreateTemporaryTableStatement preserve()
    {
        this.preserve = true;
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    protected int executeStatement(boolean printLogs)
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            log("Executing: " + sql,
                printLogs);
            result = statement.executeUpdate();

            if (this.asCopySelect != null && this.copyData)
            {
                this.db.insert().into("SESSION." + this.name).from(this.asCopySelect).execute(printLogs);
            }

            endExecutionTime();

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            handleSuccess(result);
        }
        catch (SQLException e)
        {
            result = handleFail(new SqlExecutionException(e.getMessage(), sql, e));
        }

        return result;
    }

    /**
     * Returns the string representing this statement.
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + this.name + " (";

        if (this.asCopySelect != null)
        {
            var resultSet = this.asCopySelect.execute();
            var cols = resultSet.getColumnTypes();
            String name;
            SqlType type;
            Column column;

            for (Entry<String, String> col : cols)
            {
                name = col.getKey();
                type = SqlType.convert(col.getValue());
                column = new Column(name, type);

                if (type.equals(SqlType.VARCHAR))
                {
                    column.size(9999);
                }

                column(column);
            }
        }

        for (Column col : this.tableColumns)
        {
            sql += col.toString() + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);
        sql += ")";

        if (this.preserve)
        {
            sql += " ON COMMIT PRESERVE ROWS";
        }

        sql += " NOT LOGGED";

        return sql;
    }
}
