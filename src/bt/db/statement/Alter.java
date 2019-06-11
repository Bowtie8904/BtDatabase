package bt.db.statement;

import bt.db.DatabaseAccess;
import bt.db.statement.impl.AlterTableStatement;

/**
 * Class offering methods to create different ALTER statements.
 * 
 * @author &#8904
 */
public class Alter extends SqlStatement<Create>
{
    /**
     * Creates a new instance.
     * 
     * @param db
     *            The database that should be used to execute the created statement.
     */
    public Alter(DatabaseAccess db)
    {
        super(db);
    }

    /**
     * Creates an alter table statement for the table with the given name.
     * 
     * @param name
     *            The name of the table that should be altered.
     * @return The created {@link AlterTableStatement}.
     */
    public AlterTableStatement table(String name)
    {
        return new AlterTableStatement(this.db, name);
    }
}