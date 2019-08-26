package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;

/**
 * Represents an SQL truncate table statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class TruncateTableStatement extends SqlModifyStatement<TruncateTableStatement, TruncateTableStatement>
{
    /**
     * @param db
     */
    public TruncateTableStatement(DatabaseAccess db, String table)
    {
        super(db);
        this.statementKeyword = "TRUNCATE TABLE";
        this.tables = new String[]
            {
              table
            };
    }

    /**
     * Defines a data modifying statement (insert, update, delete, ...) which will be executed if there was an error
     * during the execution of the original insert statement.
     *
     * @param onFail
     *            The SqlModifyStatement to execute instead.
     * @return This instance for chaining.
     */
    public TruncateTableStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
            {
                return onFail.execute();
            };

        return this;
    }

    /**
     * Defines a BiFunction that will be executed if there was an error during the execution of this statement.
     *
     * <p>
     * The first parameter (TruncateTableStatement) will be this statement instance, the second one is the SQLException
     * that caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public TruncateTableStatement onFail(BiFunction<TruncateTableStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
        return this;
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
            log("Executing: " + sql,
                printLogs);

            result = statement.executeUpdate();
        }
        catch (SQLException e)
        {
            if (this.onFail != null)
            {
                result = this.onFail.apply(this,
                                           e);
            }
            else
            {
                DatabaseAccess.log.print(sql);
                DatabaseAccess.log.print(e);
                result = -1;
            }
        }

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " ";
        sql += this.tables[0];
        return sql;
    }
}