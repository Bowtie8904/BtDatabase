package bt.db.statement;

import bt.db.DatabaseAccess;
import bt.db.statement.impl.CreateProcedureStatement;
import bt.db.statement.impl.CreateTableStatement;
import bt.db.statement.impl.CreateTriggerStatement;

/**
 * @author &#8904
 *
 */
public class Create extends SqlStatement<Create>
{
    /**
     * @param db
     */
    public Create(DatabaseAccess db)
    {
        super(db);

    }

    public CreateTriggerStatement trigger(String name)
    {
        return new CreateTriggerStatement(this.db, name);
    }

    public CreateTableStatement table(String name)
    {
        return new CreateTableStatement(this.db, name);
    }

    public CreateProcedureStatement procedure(String name)
    {
        return new CreateProcedureStatement(this.db, name);
    }
}