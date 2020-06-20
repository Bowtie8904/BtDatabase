package bt.db.statement.clause;

import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.constants.Generated;
import bt.db.constants.Index;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.statement.clause.foreign.ColumnForeignKey;
import bt.db.statement.clause.foreign.ForeignKey;
import bt.db.statement.impl.CreateStatement;

/**
 * Represents a column in a CREATE statement.
 *
 * @author &#8904
 */
public class Column
{
    /** The maximum size of a column comment. */
    public static final int COMMENT_SIZE = 120;

    /** The statement that created this column. */
    private CreateStatement statement;

    /** The sql value type of this column. */
    private SqlType type;

    /** The name of this column. */
    private String name;

    /** Indicates whether this column contains a primary key. true = primary key, false = not a primary key. */
    private boolean primaryKey;

    /** Defines the generate behavior. */
    private GenerationClause generated;

    /** The pameters to create an index for this column. */
    private Index[] indexParams = new Index[] {};

    private boolean shouldIndex;

    /**
     * Indicates whether a value in this column can ever be NULL. true = value can't be null, false = value can be null.
     */
    private boolean notNull;

    /** Indicates whether values in this column have to be unique. */
    private boolean unique;

    /**
     * Defines the default value that will be inserted if no other value was used explicitly. If this is not set, NULL
     * will be inserted.
     */
    private String defaultValue;

    /**
     * Indicates by how much an identity columns value should be increased on each insert. Leaving this at < 0 means
     * that no explicit inrement statement will be used and the the value will be increased by 1 each time.
     */
    private int autoIncrement = -1;

    /**
     * Contains the sizes of this column (for exmaple for VARCHAR values). Currently NUMERIC or DECIMAL types are not
     * supported this will remain an array though for future implementations.
     */
    private int[] size = new int[] {};

    /** The comment on this column which will be added to the COLLUMN_COMMENTS table. */
    private String comment;

    /** Defined foreign keys for this column. */
    private List<ColumnForeignKey> foreignKeys;

    /** Defined checks for this column. */
    private List<Check> checks;

    public static Column column(String name, SqlType type)
    {
        return new Column(name, type);
    }

    /**
     * Creates a new instance and initializes the fields.
     *
     * @param statement
     *            The statement that created this column.
     * @param name
     *            The name of this column.
     * @param type
     *            The sql type of this column.
     */
    public Column(CreateStatement statement, String name, SqlType type)
    {
        this.statement = statement;
        this.name = name;
        this.type = type;
    }

    /**
     * Creates a new instance and initializes the fields.
     *
     * @param name
     *            The name of this column.
     * @param type
     *            The sql type of this column.
     */
    public Column(String name, SqlType type)
    {
        this.name = name;
        this.type = type;
    }

    public void setStatement(CreateStatement statement)
    {
        this.statement = statement;
    }

    /**
     * Sets the comment of this column.
     *
     * <p>
     * If the given String is longer than {@link #COMMENT_SIZE} only a substring (0 - COMMENT_SIZE) will be used.
     * </p>
     *
     * @param text
     *            The comment.
     * @return This instance for chaining.
     */
    public Column comment(String text)
    {
        if (text.length() > COMMENT_SIZE)
        {
            text = text.substring(0,
                                  COMMENT_SIZE);
        }

        this.comment = text;
        return this;
    }

    /**
     * Returns the comment that was set for this column.
     *
     * @return The set comment or null.
     */
    public String getComment()
    {
        if (this.comment == null)
        {
            this.comment = generateDefaultComment();
        }

        return this.comment;
    }

    private String generateDefaultComment()
    {
        String defaultComment = "";

        if (isPrimaryKey())
        {
            defaultComment += "primary key, ";
        }

        if (isNotNull())
        {
            defaultComment += "not null, ";
        }

        if (isUnique())
        {
            defaultComment += "unique, ";
        }

        if (this.generated != null)
        {
            if (this.generated.getGenerationType() == Generated.ALWAYS)
            {
                defaultComment += "always ";
            }
            else if (this.generated.getGenerationType() == Generated.DEFAULT)
            {
                defaultComment += "default ";
            }

            if (isIdentity())
            {
                defaultComment += "generated as identity, incremented by " + getAutoIncrement() + ", ";
            }
            else
            {
                defaultComment += this.generated.getValueDetail();
            }
        }

        if (getDefaultValue() != null)
        {
            defaultComment += "default = " + getDefaultValue() + ", ";
        }

        if (this.foreignKeys != null)
        {
            for (ForeignKey fk : this.foreignKeys)
            {
                defaultComment += "foreign key (" + fk.getName() + "), ";
            }
        }

        if (this.checks != null)
        {
            for (Check check : this.checks)
            {
                defaultComment += "check (" + check.getName() + "), ";
            }
        }

        if (!defaultComment.isBlank())
        {
            defaultComment = defaultComment.substring(0, defaultComment.length() - 2);
        }

        return defaultComment;
    }

    /**
     * Sets the size of this column.
     *
     * <p>
     * This has to be used on VARCHAR columns to limit the length of inserted values.
     * </p>
     *
     * @param values
     *            The sizes. For VARCHAR only one size can be given.
     * @return This instance for chaining.
     */
    public Column size(int... values)
    {
        this.size = values;
        return this;
    }

    /**
     * Marks this column as primary key for the table.
     *
     * <p>
     * This means that it can never be null, needs to be unique and has to be filled on insert. Either explicitly with a
     * SET or implicitly by marking this column {@link #asIdentity(Generated)} and using {@link Generated#ALWAYS}
     * (INTEGER or BIGINT(Long) columns only).
     * </p>
     *
     * @return This instance for chaining.
     */
    public Column primaryKey()
    {
        this.primaryKey = true;
        return this;
    }

    /**
     * Marks this column as unique. Meaning that all values inside this column need to be column unique.
     *
     * @return This instance for chaining.
     */
    public Column unique()
    {
        this.unique = true;
        this.notNull = true;
        return this;
    }

    /**
     * Sets the generation type for columns whichs value does not have to be explicitely inserted.
     *
     * @param generated
     * @return
     */
    public GenerationClause generated(Generated generated)
    {
        var clause = new GenerationClause(generated, this);

        if (clause.isIdentity() && this.autoIncrement == -1)
        {
            this.autoIncrement = 1;
        }

        this.generated = clause;

        return clause;
    }

    /**
     * Creates an index for this column upon creation by using the given index parameters.
     *
     * @param params
     * @return
     */
    public Column index(Index... params)
    {
        this.indexParams = params;
        this.shouldIndex = true;
        return this;
    }

    /**
     * Indicates whether this column has been marked to receive an index.
     *
     * @return
     */
    public boolean shouldIndex()
    {
        return this.shouldIndex;
    }

    /**
     * Gets the specified parameters for the index that should be created.
     *
     * @return
     */
    public Index[] getIndexParams()
    {
        return this.indexParams;
    }

    /**
     * Indicates whether this columns value should be generated as identity.
     *
     * <p>
     * The behavior is highly dependent on {@link #generated} as it defines whether the value is always uniquely
     * generated or only generated if it is not inserted explicitly.
     * </p>
     *
     * @return true = generate, false = don't generate.
     */
    public boolean isIdentity()
    {
        return this.generated != null ? this.generated.isIdentity() : false;
    }

    /**
     * Indicates whether this column should be marked as unique.
     *
     * @return true = unique, false = not unique.
     */
    public boolean isUnique()
    {
        return this.unique;
    }

    /**
     * Indicates whether values in this collumn can be null.
     *
     * @return true = can be null, false = can't be null.
     */
    public boolean isNotNull()
    {
        return this.notNull;
    }

    /**
     * Returns the number by which this fields value will be incremented if this collumn is marked as an identity.
     *
     * @return The number that is used to increment the collumns value by each time.
     */
    public int getAutoIncrement()
    {
        return this.autoIncrement;
    }

    /**
     * Defines the number by which the value, that is automatically generated, should be incremented.
     *
     * <p>
     * This only has an effect on column marked {@link #asIdentity(Generated)}.
     * </p>
     *
     * @param n
     *            The number by which the identity value should be incremented each insert.
     * @return This instance for chaining.
     */
    public Column autoIncrement(int n)
    {
        this.autoIncrement = n;
        return this;
    }

    /**
     * Marks this column as non nullable.
     *
     * @return This instance for chaining.
     */
    public Column notNull()
    {
        this.notNull = true;
        return this;
    }

    /**
     * Sets the default value of this column.
     *
     * @param defaultValue
     *            The value that should be used if nothing else is specified.
     * @return This instance for chaining.
     */
    public Column defaultValue(Object defaultValue)
    {
        if (this.type == SqlType.VARCHAR)
        {
            this.defaultValue = "'" + defaultValue.toString() + "'";
        }
        else if (this.type == SqlType.DATE || this.type == SqlType.TIME || this.type == SqlType.TIMESTAMP)
        {
            if (defaultValue instanceof SqlValue)
            {
                this.defaultValue = defaultValue.toString();
            }
            else
            {
                this.defaultValue = "'" + defaultValue.toString() + "'";
            }
        }
        else
        {
            this.defaultValue = defaultValue.toString();
        }

        return this;
    }

    /**
     * Gets the create statement that this column is a part of.
     *
     * @return
     */
    public CreateStatement getStatement()
    {
        return this.statement;
    }

    /**
     * Indicates whether this column is used as a primary key.
     *
     * @return true = primary key, false = not primary key.
     */
    public boolean isPrimaryKey()
    {
        return this.primaryKey;
    }

    /**
     * Returns the name of this column.
     *
     * @return The name.
     */
    public String getName()
    {
        return this.name;
    }

    /**
     * Returns the sql type of this column.
     *
     * @return The type.
     */
    public SqlType getType()
    {
        return this.type;
    }

    /**
     * Gets the String representation of the default value.
     *
     * @return The value.
     */
    public String getDefaultValue()
    {
        return this.defaultValue;
    }

    public GenerationClause getGenerationCaluse()
    {
        return this.generated;
    }

    /**
     * Adds a new column foreign key.
     *
     * @return This instance for chaining.
     */
    public Column foreignKey(ColumnForeignKey foreignKey)
    {
        if (this.foreignKeys == null)
        {
            this.foreignKeys = new ArrayList<>();
        }

        foreignKey.setColumn(this);
        this.foreignKeys.add(foreignKey);

        return this;
    }

    public void saveColumnData(DatabaseAccess db)
    {
        String dataType = this.type.toString();

        if (this.type == SqlType.VARCHAR)
        {
            dataType += " (";

            for (int i : this.size)
            {
                dataType += i + ", ";
            }

            dataType = dataType.substring(0, dataType.length() - 2);

            dataType += ")";
        }

        String generationInfo = "";
        if (this.generated != null)
        {
            if (this.generated.getGenerationType() == Generated.ALWAYS)
            {
                generationInfo += "always ";
            }
            else if (this.generated.getGenerationType() == Generated.DEFAULT)
            {
                generationInfo += "default ";
            }

            if (isIdentity())
            {
                generationInfo += "as identity";

                if (this.autoIncrement > 0)
                {
                    generationInfo += " (START WITH 1, INCREMENT BY " + this.autoIncrement + ")";
                }
            }
            else
            {
                generationInfo += this.generated.getValueDetail();
            }
        }

        String foreignKeyStr = "";
        if (this.foreignKeys != null)
        {
            for (ForeignKey fk : this.foreignKeys)
            {
                foreignKeyStr += fk.getName() + ", ";
            }

            if (!foreignKeyStr.isBlank())
            {
                foreignKeyStr = foreignKeyStr.substring(0, foreignKeyStr.length() - 2);
            }
        }

        String checkStr = "";
        if (this.checks != null)
        {
            for (Check check : this.checks)
            {
                checkStr += check.getName() + ", ";
            }

            if (!checkStr.isBlank())
            {
                checkStr = checkStr.substring(0, checkStr.length() - 2);
            }
        }

        db.insert()
          .into(DatabaseAccess.COLUMN_DATA)
          .set("instanceID", db.getInstanceID())
          .set("table_name", this.statement.getName().toUpperCase())
          .set("column_name", this.name.toUpperCase())
          .set("data_type", dataType)
          .set("primary_key", this.primaryKey)
          .set("is_identity", isIdentity())
          .set("generation", generationInfo)
          .set("not_null", this.notNull)
          .set("is_unique", this.unique)
          .set("default_value", this.defaultValue == null ? "" : this.defaultValue)
          .set("foreign_keys", foreignKeyStr)
          .set("checks", checkStr)
          .set("comment", getComment())
          .onDuplicateKey(db.update(DatabaseAccess.COLUMN_DATA)
                            .set("instanceID", db.getInstanceID())
                            .set("data_type", dataType)
                            .set("primary_key", this.primaryKey)
                            .set("is_identity", isIdentity())
                            .set("generation", generationInfo)
                            .set("not_null", this.notNull)
                            .set("is_unique", this.unique)
                            .set("default_value", this.defaultValue == null ? "" : this.defaultValue)
                            .set("foreign_keys", foreignKeyStr)
                            .set("checks", checkStr)
                            .set("comment", getComment())
                            .set("updated", SqlValue.SYSTIMESTAMP, SqlType.TIMESTAMP))
          .execute();
    }

    /**
     * Adds a new column check.
     *
     * @return This instance for chaining.
     */
    public Column check(Check check)
    {
        if (this.checks == null)
        {
            this.checks = new ArrayList<>();
        }

        this.checks.add(check);

        return this;
    }

    /**
     * Returns the String representing this column creation query.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.name + " " + this.type;

        if (this.size.length > 0)
        {
            sql += "(";

            for (int i : this.size)
            {
                sql += i + ", ";
            }

            sql = sql.substring(0,
                                sql.length() - 2);
            sql += ")";
        }

        if (this.defaultValue != null)
        {
            sql += " DEFAULT " + this.defaultValue;
        }

        if (this.notNull)
        {
            sql += " NOT NULL";
        }

        if (this.generated != null)
        {
            sql += " " + this.generated.toString();

            if (this.autoIncrement > 0)
            {
                sql += " (START WITH 1, INCREMENT BY " + this.autoIncrement + ")";
            }
        }

        if (this.checks != null)
        {
            for (Check check : this.checks)
            {
                sql += " " + check.toString();
            }
        }

        if (this.foreignKeys != null)
        {
            for (ForeignKey fk : this.foreignKeys)
            {
                sql += " " + fk.toString();
            }
        }

        if (this.unique)
        {
            sql += ", CONSTRAINT " + this.statement.getName() + "_" + this.name + "_UQ UNIQUE(" + this.name + ")";
        }

        return sql;
    }
}