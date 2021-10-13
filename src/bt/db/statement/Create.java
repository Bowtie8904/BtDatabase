package bt.db.statement;

import bt.db.DatabaseAccess;
import bt.db.statement.impl.*;

/**
 * Class offering methods to create different CREATE statements.
 *
 * @author &#8904
 */
public class Create extends SqlStatement<Create>
{
    /**
     * Creates a new instance.
     *
     * @param db The database that should be used to execute the created statement.
     */
    public Create(DatabaseAccess db)
    {
        super(db);

    }

    /**
     * Creates a create trigger statementwith the given name.
     *
     * @param name The name of the trigger that should be created.
     * @return The created {@link CreateTriggerStatement}.
     */
    public CreateTriggerStatement trigger(String name)
    {
        return new CreateTriggerStatement(this.db,
                                          name);
    }

    /**
     * Creates a create temporary table statement for the table with the given name.
     *
     * <p>
     * The table will be dropped when the connection to the database is closed.
     * </p>
     *
     * @param name The name of the table that should be created.
     * @return The created {@link CreateTemporaryTableStatement}.
     */
    public CreateTemporaryTableStatement temporaryTable(String name)
    {
        return new CreateTemporaryTableStatement(this.db,
                                                 name);
    }

    /**
     * Creates a create table statement for the table with the given name.
     *
     * @param name The name of the table that should be created.
     * @return The created {@link CreateTableStatement}.
     */
    public CreateTableStatement table(String name)
    {
        return new CreateTableStatement(this.db,
                                        name);
    }

    /**
     * Creates a create procedure statement with the given name.
     *
     * @param name The name of the procedure that should be created.
     * @return The created {@link CreateProcedureStatement}.
     */
    public CreateProcedureStatement procedure(String name)
    {
        return new CreateProcedureStatement(this.db,
                                            name);
    }

    /**
     * Creates a create function statement with the given name.
     *
     * @param name The name of the procedure that should be created.
     * @return The created {@link CreateFunctionStatement}.
     */
    public CreateFunctionStatement function(String name)
    {
        return new CreateFunctionStatement(this.db,
                                           name);
    }

    public CreateIndexStatement index(String name)
    {
        return new CreateIndexStatement(this.db,
                                        name);
    }

    public CreateViewStatement view(String name)
    {
        return new CreateViewStatement(this.db,
                                       name);
    }
}