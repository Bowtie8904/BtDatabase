package bt.db.statement;

import bt.db.DatabaseAccess;
import bt.db.statement.impl.AlterTableStatement;

/**
 * @author &#8904
 *
 */
public class Alter extends SqlStatement<Create>
{
    public Alter(DatabaseAccess db)
    {
        super(db);
    }

    public AlterTableStatement table(String name)
    {
        return new AlterTableStatement(this.db, name);
    }
}