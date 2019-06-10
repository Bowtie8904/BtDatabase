package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import bt.db.DatabaseAccess;
import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.statement.clause.TableColumn;

/**
 * Represents an SQL alter statement which can be extended through method chaining.
 * 
 * @author &#8904
 */
public class AlterTableStatement extends CreateStatement<AlterTableStatement, AlterTableStatement>
{
    private TableColumn newColumn;

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
        super(db, name);
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
    public TableColumn<AlterTableStatement> column(String name, SqlType type)
    {
        TableColumn<AlterTableStatement> column = new TableColumn<AlterTableStatement>(this, name, type);
        return column;
    }

    /**
     * Adds the column to this table.
     * 
     * @see bt.db.statement.impl.CreateStatement#addColumn(bt.db.statement.clause.TableColumn)
     */
    @Override
    public AlterTableStatement addColumn(TableColumn column)
    {
        this.newColumn = column;
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#commit()
     */
    @Override
    public AlterTableStatement commit()
    {
        return (AlterTableStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public AlterTableStatement unprepared()
    {
        return (AlterTableStatement)super.unprepared();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    public int execute()
    {
        return execute(false);
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    public int execute(boolean printLogs)
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            log("Executing: " + sql, printLogs);
            statement.executeUpdate();
            result = 1;

            if (this.newColumn != null)
            {
                if (this.newColumn.getComment() != null)
                {
                    db.insert()
                            .into("column_comments")
                            .set("table_name", this.name.toUpperCase())
                            .set("column_name", this.newColumn.getName().toUpperCase())
                            .set("column_comment", this.newColumn.getComment())
                            .execute(printLogs);
                }
                else
                {
                    StringBuilder comment = new StringBuilder();

                    if (this.newColumn.isPrimaryKey())
                    {
                        comment.append("primary key, ");
                    }

                    if (this.newColumn.isNotNull())
                    {
                        comment.append("not null, ");
                    }

                    if (this.newColumn.isIdentity())
                    {
                        if (this.newColumn.getGenerationType() == Generated.ALWAYS)
                        {
                            comment.append("always ");
                        }
                        else if (this.newColumn.getGenerationType() == Generated.DEFAULT)
                        {
                            comment.append("default ");
                        }
                        comment.append("generated, incremented by ")
                                .append(this.newColumn.getAutoIncrement())
                                .append(", ");
                    }

                    if (this.newColumn.getDefaultValue() != null)
                    {
                        comment.append("default = ")
                                .append(this.newColumn.getDefaultValue())
                                .append(", ");
                    }

                    if (comment.length() != 0)
                    {
                        db.insert()
                                .into("column_comments")
                                .set("table_name", this.name.toUpperCase())
                                .set("column_name", this.newColumn.getName().toUpperCase())
                                .set("column_comment", comment.substring(0, comment.length() - 2))
                                .execute(printLogs);
                    }
                    else
                    {
                        db.insert()
                                .into("column_comments")
                                .set("table_name", this.name.toUpperCase())
                                .set("column_name", this.newColumn.getName().toUpperCase())
                                .execute(printLogs);
                    }
                }
            }

            if (this.shouldCommit)
            {
                this.db.commit();
            }
        }
        catch (SQLException e)
        {
            if (this.onFail != null)
            {
                result = this.onFail.apply(this, e);
            }
            else
            {
                DatabaseAccess.log.print(e);
                result = -1;
            }
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