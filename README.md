# BtDatabase
### A Java library to make use of the JDBC API via intuitive method chaining. 

**This library is not a full on DBMS.** It uses the [Apache Derby DB](https://db.apache.org/derby/) under the hood and should be seen as an additional bridge between you and the JDBC API to allow intuitive method chaining while also adding some useful utility features such as trigger listeners and automated persisting of entire objects.

### Contents
- [How to get started](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#how-to-get-started)
  - [Requirements](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#requirements)
  - [Add BtDatabase via Maven](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#add-btdatabase-via-maven)
  - [Create a new database](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#create-a-new-database)
    - [Database configuration](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#database-configuration)
    - [DatabaseAccess class](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#databaseaccess-class)


## How to get started

  ### Requirements
  - Java 11+


  ### Add BtDatabase via Maven
  To add this library to your project via Maven simply add the follosing lines to your pom.xml file.

  ```xml
  <repositories>
          <repository>
              <id>jitpack.io</id>
              <url>https://jitpack.io</url>
          </repository>
      </repositories>
    <dependencies>
      <dependency>
          <groupId>com.github.Bowtie8904</groupId>
          <artifactId>BtDatabase</artifactId>
          <version>1.0</version>
      </dependency>
    </dependencies>
  ``` 

  ### Create a new database
  #### Database configuration
  The class `DatabaseConfiguration` allows you to create the perfect connection String for your desired database.
  A simple configuration would look like this
  ```Java
  DatabaseConfiguration config = new DatabaseConfiguration()
                                                            .path("./db") // 1
                                                            .create() // 2
                                                            .useUnicode() // 3
                                                            .characterEncoding("utf8") // 4
                                                            .autoReconnect(); // 5
  ```
  **What it does:**
  1. sets the path of the database to the folder `db` within your projects folder
  2. attempts to create the database if it does not exist yet
  3. enables the database to use unicode
  4. sets the character encoding to UTF-8
  5. attempts to automatically reconnect to the database if needed
  
  Internally this configuration will produce the following connection URL
  `jdbc:derby:;databaseName=./db;useUnicode=true;create=true;characterEncoding=utf8;autoReconnect=true`
  
  
  #### DatabaseAccess class
  `DatabaseAccess` is the root class for all databse classes. If you want to implement an entirely new system on how to handle the database access, then you should extend this class. This is only recommended if you really know what is going on inside the library. For most cases it will be sufficient to extend `EmbeddedDatabase` or `RemoteDatabase` as they already implement a fully functioning trigger system.


  **EmbeddedDatabase or RemoteDatabase?**
  
  The main difference between thew two implementations is way how they handle triggers. The `EmbeddedDatabase` class will install the jar file of your program to the database, so that triggers will call the internal methods directly. The `RemoteDatabase` will work with a trigger table where new trigger messages are added and read from on a set interval. 
  If you can, always go for an `EmbeddedDatabase` implementation as triggers will be reported much faster which can increase your programs performance based on trigger usage.

Since there is no difference in extending one or the other, further examples will always use an `EmbeddedDatabase`.


**Extending the root classes**

All three classes (`DatabaseAccess`, `EmbeddedDatabase` and `RemoteDatabase`) are abstract.

When extending `EmbeddedDatabase` you will need to implement the abstract method `createTables`. Inside that method you should put all code that creates a tables of the database if they don't exist yet.

An example would look like this:
```Java
public class Database extends LocalDatabase
{
    public Database(DatabaseConfiguration config)
    {
        super(config);
    }

    /**
     * @see bowt.db.DatabaseAccess#createTables()
     */
    @Override
    protected void createTables()
    {
        create().table("testtable")
                .execute(true);

        commit();
    }
}
```
See [this]() for more information on how to create tables.
