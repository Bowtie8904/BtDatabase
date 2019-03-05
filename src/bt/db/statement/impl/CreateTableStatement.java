package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.LocalDatabase;
import bt.db.RemoteDatabase;
import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.clause.TableColumn;

/**
 * @author &#8904
 *
 */
public class CreateTableStatement extends CreateStatement<CreateTableStatement, CreateTableStatement>
{
    private boolean createDefaultTriggers = true;
    private String identity;
    private List<TableColumn<CreateTableStatement>> tableColumns;

    public CreateTableStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE TABLE";
        this.tableColumns = new ArrayList<>();
    }

    public TableColumn<CreateTableStatement> column(String name, SqlType type)
    {
        TableColumn<CreateTableStatement> column = new TableColumn<CreateTableStatement>(this, name, type);
        return column;
    }

    @Override
    public CreateTableStatement addColumn(TableColumn column)
    {
        this.tableColumns.add(column);
        return this;
    }

    @Override
    public CreateTableStatement commit()
    {
        return (CreateTableStatement)super.commit();
    }

    @Override
    public CreateTableStatement unprepared()
    {
        return (CreateTableStatement)super.unprepared();
    }

    public CreateTableStatement createDefaultTriggers(boolean defaultTriggers)
    {
        this.createDefaultTriggers = defaultTriggers;
        return this;
    }

    private void createTriggers(boolean printLogs)
    {
        if (db instanceof LocalDatabase)
        {
            db.create().trigger(this.name + "_t_delete")
                    .after("delete")
                    .on(this.name)
                    .oldAs("oldRow")
                    .forEachRow()
                    .call("onDelete")
                    .with(db.getID(), this.name.toUpperCase(), this.identity, new ColumnEntry("oldRow", this.identity))
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_insert")
                    .after("insert")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .call("onInsert")
                    .with(db.getID(), this.name.toUpperCase(), this.identity, new ColumnEntry("newRow", this.identity))
                    .replace()
                    .execute(printLogs);

            db.create().trigger(this.name + "_t_update")
                    .after("update")
                    .on(this.name)
                    .newAs("newRow")
                    .forEachRow()
                    .call("onUpdate")
                    .with(db.getID(), this.name.toUpperCase(), this.identity, new ColumnEntry("newRow", this.identity))
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

    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + this.name + " (";

        String primary = "";

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

        return sql;
    }
}