package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.EmbeddedDatabase;
import bt.db.RemoteDatabase;
import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.clause.TableColumn;

/**
 * Represents an SQL create table statement which can be extended through method chaining.
 * 
 * @author &#8904
 */
public class CreateTableStatement extends CreateStatement<CreateTableStatement, CreateTableStatement>
{
    /** Indictaes whether the default triggers should be created for this table. */
    private boolean createDefaultTriggers = true;

    /** The identity field used for the default triggers. */
    private String identity;

    /** The columns that will be added to the new table. */
    private List<TableColumn<CreateTableStatement>> tableColumns;

    /** The select to create a copy table of. */
    private SelectStatement asCopySelect;

    /** Indicates whether a table copy should be created with data. */
    private boolean copyData = true;

    /**
     * Creates a new instance.
     * 
     * @param db
     *            The database that should be used for the statement.
     * @param name
     *            The name of the table that should be created.
     */
    public CreateTableStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE TABLE";
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
    public TableColumn<CreateTableStatement> column(String name, SqlType type)
    {
        TableColumn<CreateTableStatement> column = new TableColumn<CreateTableStatement>(this, name, type);
        return column;
    }

    /**
     * Adds the column to this table.
     * 
     * @see bt.db.statement.impl.CreateStatement#addColumn(bt.db.statement.clause.TableColumn)
     */
    @Override
    public CreateTableStatement addColumn(TableColumn column)
    {
        this.tableColumns.add(column);
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#commit()
     */
    @Override
    public CreateTableStatement commit()
    {
        return (CreateTableStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public CreateTableStatement unprepared()
    {
        return (CreateTableStatement)super.unprepared();
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
    public CreateTableStatement as(SelectStatement select)
    {
        this.asCopySelect = select.unprepared();
        this.createDefaultTriggers = false;
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
    public CreateTableStatement asCopyOf(String table)
    {
        this.asCopySelect = this.db.select()
                .from(table)
                .unprepared()
                .onLessThan(1, (i, set) -> {
                    return set;
                });
        this.createDefaultTriggers = false;
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
    public CreateTableStatement withData(boolean copyData)
    {
        this.copyData = copyData;
        return this;
    }

    /**
     * Indictaes whether the default triggers (insert, update, delete) should be created.
     * 
     * @param defaultTriggers
     * @return
     */
    public CreateTableStatement createDefaultTriggers(boolean defaultTriggers)
    {
        this.createDefaultTriggers = defaultTriggers;
        return this;
    }

    private void createTriggers(boolean printLogs)
    {
        if (db instanceof EmbeddedDatabase)
        {
            db.create().trigger(this.name + "_t_delete")
                    .after("delete")
                    .on(this.name)
                    .oldAs("oldRow")
                    .forEachRow()
                    .call("onDelete")
                    .with(db.getInstanceID(), this.name.toUpperCase(), this.identity, new ColumnEntry("oldRow", this.identity))
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_insert")
                    .after("insert")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .call("onInsert")
                    .with(db.getInstanceID(), this.name.toUpperCase(), this.identity, new ColumnEntry("newRow", this.identity))
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_update")
                    .after("update")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .call("onUpdate")
                    .with(db.getInstanceID(), this.name.toUpperCase(), this.identity, new ColumnEntry("newRow", this.identity))
                    .replace()
                    .execute(printLogs);
        }
        else if (db instanceof RemoteDatabase)
        {
            db.create().trigger(this.name + "_t_delete")
                    .after("delete")
                    .on(this.name)
                    .oldAs("oldRow")
                    .forEachRow()
                    .execute(
                            "INSERT INTO recent_triggers (triggerType, tableName, rowIdFieldName, idRow) values ('delete', '"
                                    + this.name.toUpperCase() + "', '" + this.identity + "', oldRow."
                                    + this.identity
                                    + ")")
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_insert")
                    .after("insert")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .execute(
                            "INSERT INTO recent_triggers (triggerType, tableName, rowIdFieldName, idRow) values ('insert', '"
                                    + this.name.toUpperCase() + "', '" + this.identity + "', newRow."
                                    + this.identity
                                    + ")")
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_update")
                    .after("update")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .execute(
                            "INSERT INTO recent_triggers (triggerType, tableName, rowIdFieldName, idRow) values ('update', '"
                                    + this.name.toUpperCase() + "', '" + this.identity + "', newRow."
                                    + this.identity
                                    + ")")
                    .replace()
                    .execute(printLogs);
        }
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

            if (this.createDefaultTriggers)
            {
                createTriggers(printLogs);
            }

            if (this.asCopySelect == null)
            {
                for (TableColumn col : this.tableColumns)
                {
                    if (col.getComment() != null)
                    {
                        db.insert()
                                .into("column_comments")
                                .set("table_name", this.name.toUpperCase())
                                .set("column_name", col.getName().toUpperCase())
                                .set("column_comment", col.getComment())
                                .execute(printLogs);
                    }
                    else
                    {
                        String comment = "";

                        if (col.isPrimaryKey())
                        {
                            comment += "primary key, ";
                        }

                        if (col.isNotNull())
                        {
                            comment += "not null, ";
                        }

                        if (col.isUnique())
                        {
                            comment += "unique, ";
                        }

                        if (col.isIdentity())
                        {
                            if (col.getGenerationType() == Generated.ALWAYS)
                            {
                                comment += "always ";
                            }
                            else if (col.getGenerationType() == Generated.DEFAULT)
                            {
                                comment += "default ";
                            }
                            comment += "generated, incremented by " + col.getAutoIncrement() + ", ";
                        }

                        if (col.getDefaultValue() != null)
                        {
                            comment += "default = " + col.getDefaultValue() + ", ";
                        }

                        if (!comment.isEmpty())
                        {
                            comment = comment.substring(0, comment.length() - 2);

                            db.insert()
                                    .into("column_comments")
                                    .set("table_name", this.name.toUpperCase())
                                    .set("column_name", col.getName().toUpperCase())
                                    .set("column_comment", comment)
                                    .execute(printLogs);
                        }
                        else
                        {
                            db.insert()
                                    .into("column_comments")
                                    .set("table_name", this.name.toUpperCase())
                                    .set("column_name", col.getName().toUpperCase())
                                    .execute(printLogs);
                        }
                    }
                }
            }
            else if (this.copyData)
            {
                this.db.insert().into(this.name).from(this.asCopySelect).execute(printLogs);
            }

            result = 1;

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
        String sql = this.statementKeyword + " " + this.name + " (";
        String primary = "";

        if (this.asCopySelect == null)
        {
            for (TableColumn col : this.tableColumns)
            {
                if (col.isPrimaryKey())
                {
                    primary += col.getName() + ", ";
                }

                if (col.isIdentity() && col.getGenerationType() == Generated.ALWAYS)
                {
                    if (this.identity == null
                            && (col.getType() == SqlType.LONG))
                    {
                        this.identity = col.getName();
                    }
                }

                sql += col.toString() + ", ";
            }

            sql = sql.substring(0, sql.length() - 2);

            if (primary.length() != 0)
            {
                primary = primary.substring(0, primary.length() - 2);
                sql += ", PRIMARY KEY (" + primary + ")";
            }

            if (this.identity == null)
            {
                // we always need a unique identity of type long for default triggers and automated persisting
                TableColumn defaultPrimary = column("DEFAULT_ID", SqlType.LONG)
                        .notNull()
                        .asIdentity(Generated.ALWAYS)
                        .autoIncrement(1);

                this.identity = "DEFAULT_ID";

                this.tableColumns.add(defaultPrimary);

                return toString();
            }

            sql += ")";
        }
        else
        {
            sql = this.statementKeyword + " " + this.name + " AS " + this.asCopySelect.toString() + " WITH NO DATA";
        }

        return sql;
    }
}