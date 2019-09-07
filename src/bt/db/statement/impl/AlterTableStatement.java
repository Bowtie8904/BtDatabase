package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.clause.Column;
import bt.db.statement.result.SqlResult;

/**
 * Represents an SQL alter statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class AlterTableStatement extends CreateStatement<AlterTableStatement, AlterTableStatement>
{
    private Column newColumn;
    private boolean saveObjectData = true;

    /**
     * Creates a new instance.
     *
     * @param db
     *            The database that should be used for the statement.
     * @param name
     *            The name of the table that should be altered.
     */
    public AlterTableStatement(DatabaseAccess db, String name)
    {
        super(db,
              name);
        this.statementKeyword = "ALTER TABLE";
    }

    /**
     * Adds a new column in this table which has the given name and the given sql type.
     *
     * @param name
     *            The name of the column.
     * @param type
     *            The {@link SqlType type} of the column.
     * @return The created column.
     */
    public AlterTableStatement column(Column column)
    {
        this.newColumn = column;
        column.setStatement(this);
        return this;
    }

    /**
     * Indicates whether this objects DDL should be saved in 'BT_TABLE_DATA'.
     *
     * @param saveTableData
     * @return
     */
    @Override
    public AlterTableStatement saveObjectData(boolean saveObjectData)
    {
        this.saveObjectData = saveObjectData;
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
            statement.executeUpdate();
            endExecutionTime();
            result = 1;

            if (this.saveObjectData)
            {
                this.db.select()
                       .from(DatabaseAccess.OBJECT_DATA_TABLE)
                       .where("object_name").equals(this.name.toUpperCase())
                       .first()
                       .onLessThan(1, this.db.insert()
                                             .into(DatabaseAccess.OBJECT_DATA_TABLE)
                                             .set("instanceID", this.db.getInstanceID())
                                             .set("object_name", this.name.toUpperCase())
                                             .set("object_ddl", sql + ";"))
                       .onMoreThan(0, (i, set) ->
                       {
                           SqlResult row = set.get(0);
                           String oldDDL = row.getString("object_ddl");

                           this.db.update(DatabaseAccess.OBJECT_DATA_TABLE)
                                  .set("object_ddl", oldDDL + " " + sql + ";")
                                  .set("updated", SqlValue.SYSTIMESTAMP, SqlType.TIMESTAMP)
                                  .where("object_name").equals(this.name.toUpperCase())
                                  .execute();

                           return set;
                       })
                       .execute();

                if (this.newColumn != null)
                {
                    this.newColumn.saveColumnData(this.db);
                }
            }

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
        String sql = this.statementKeyword + " " + this.name;

        if (this.newColumn != null)
        {
            sql += " ADD COLUMN " + this.newColumn.toString();
        }

        return sql;
    }
}