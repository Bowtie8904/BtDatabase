package bt.db.config;

import java.util.HashMap;
import java.util.Map;

/**
 * A class to define connection attributes for a database.
 * 
 * @author &#8904
 */
public class DatabaseConfiguration
{
    private Map<String, String> attributes;

    public DatabaseConfiguration()
    {
        this.attributes = new HashMap<>();
    }

    /**
     * Defines the path to the database.
     * 
     * @param databasePath
     * @return This instance for chaining.
     */
    public DatabaseConfiguration path(String path)
    {
        this.attributes.put("databaseName", path);
        return this;
    }

    /**
     * Sets the database to be automatically created if it does not exist.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration create()
    {
        this.attributes.put("create", "true");
        return this;
    }

    /**
     * Sets the database to use unicode.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration useUnicode()
    {
        this.attributes.put("useUnicode", "true");
        return this;
    }

    /**
     * Sets the database to use the given character encoding.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration characterEncoding(String encoding)
    {
        this.attributes.put("characterEncoding", encoding);
        return this;
    }

    /**
     * Sets the database to automatically reconnect in case of a disconnect.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration autoReconnect()
    {
        this.attributes.put("autoReconnect", "true");
        return this;
    }

    /**
     * Sets the database to use the given boot password to encrypt the database.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration bootPassword(String bootPassword)
    {
        this.attributes.put("dataEncryption", "true");
        this.attributes.put("bootPassword", bootPassword);
        return this;
    }

    /**
     * Sets the path where derby will create its logfiles.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration logPath(String logPath)
    {
        this.attributes.put("logDevice", logPath);
        return this;
    }

    /**
     * Sets the credentials that should be used to log into the database.
     * 
     * @return This instance for chaining.
     */
    public DatabaseConfiguration login(String user, String password)
    {
        this.attributes.put("user", user);
        this.attributes.put("password", password);
        return this;
    }

    /**
     * Formats a connection URL pased on the set attributes.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String url = "jdbc:derby:;";
        String value;
        boolean hasAttributes = false;

        for (String key : this.attributes.keySet())
        {
            value = this.attributes.get(key);

            if (value != null)
            {
                url += key + "=" + value + ";";
                hasAttributes = true;
            }
        }

        if (hasAttributes)
        {
            url = url.substring(0, url.length() - 1);
        }

        return url;
    }
}