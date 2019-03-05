package bt.db.statement.clause;

import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.statement.impl.CreateStatement;

/**
 * Represents a column in a CREATE statement.
 * 
 * @author &#8904
 */
public class TableColumn<T extends CreateStatement>
{
    /** The maximum size of a column comment. */
    public static final int COMMENT_SIZE = 120;

    /** The statement that created this column. */
    private T statement;

    /** The sql value type of this column. */
    private SqlType type;

    /** The name of this column. */
    private String name;

    /** Indicates whether this column contains a primary key. true = primary key, false = not a primary key. */
    private boolean primaryKey;

    /**
     * Indicates whether this columns value should be generated as identity. true = generate, false = don't generate.
     * 
     * <p>
     * The behavior is highly dependent on {@link #generated} as it defines whether the value is always uniquely
     * generated or only generated if it is not inserted explicitly.
     * </p>
     */
    private boolean asIdentity;

    /** Defines the generate behavior on column marked as {@link #asIdentity}. */
    private Generated generated;

    /**
     * Indicates whether a value in this column can ever be NULL. true = value can't be null, false = value can be
     * null.
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
    public TableColumn(T statement, String name, SqlType type)
    {
        this.statement = statement;
        this.name = name;
        this.type = type;
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
    public TableColumn<T> comment(String text)
    {
        if (text.length() > COMMENT_SIZE)
        {
            text = text.substring(0, COMMENT_SIZE);
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
        return this.comment;
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
    public TableColumn<T> size(int... values)
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
    public TableColumn<T> primaryKey()
    {
        this.primaryKey = true;
        return this;
    }

    /**
     * Marks this column as unique. Meaning that all values inside this column need to be column unique.
     * 
     * @return This instance for chaining.
     */
    public TableColumn<T> unique()
    {
        this.unique = true;
        this.notNull = true;
        return this;
    }

    /**
     * Marks this column as an identity which means that it can be generated on insert.
     * 
     * <p>
     * Only INTEGER and BIGINT (Long) typed columns can be used as identity.
     * </p>
     * 
     * <p>
     * If nothing else is specified by {@link #autoIncrement(int)} the value will be incremented by 1 each time.
     * </p>
     * 
     * @param generated
     *            Defines the behavior of the identity as either {@link Generated#ALWAYS} or {@link Generated#DEFAULT}.
     * @return This instance for chaining.
     */
    public TableColumn<T> asIdentity(Generated generated)
    {
        if (this.type != SqlType.INTEGER && this.type != SqlType.LONG)
        {
            throw new IllegalArgumentException("Non Integer or Long columns can't be generated as identity.");
        }

        this.asIdentity = true;
        this.generated = generated;
        this.autoIncrement = 1;
        return this;
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
        return this.asIdentity;
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
    public TableColumn<T> autoIncrement(int n)
    {
        this.autoIncrement = n;
        return this;
    }

    /**
     * Marks this column as non nullable.
     * 
     * @return This instance for chaining.
     */
    public TableColumn<T> notNull()
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
    public TableColumn<T> defaultValue(Object defaultValue)
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
     * Indicates whether this column is used as a primary key.
     * 
     * @return true = primary key, false = not primary key.
     */
    public boolean isPrimaryKey()
    {
        return this.primaryKey;
    }

    /**
     * Adds this column to the CREATE statement which created this instance.
     * 
     * @return The statement that created this instance.
     */
    public T add()
    {
        this.statement.addColumn(this);
        return this.statement;
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

    /**
     * Returnes the used generation type for identity collumns.
     * 
     * @return The generation type.
     */
    public Generated getGenerationType()
    {
        return this.generated;
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
            
            sql = sql.substring(0, sql.length() - 2);
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

        if (this.asIdentity)
        {
            sql += " GENERATED " + (this.generated == Generated.ALWAYS ? "ALWAYS" : "BY DEFAULT") + " AS IDENTITY";

            if (this.autoIncrement > 0)
            {
                sql += " (START WITH 1, INCREMENT BY " + this.autoIncrement + ")";
            }
        }

        if (this.unique)
        {
            sql += ", CONSTRAINT " + this.name + "_uq UNIQUE(" + this.name + ")";
        }

        return sql;
    }
}