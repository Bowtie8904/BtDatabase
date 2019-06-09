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
    - [EmbeddedDatabase or RemoteDatabase?](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#embeddeddatabase-or-remotedatabase)
    - [Extending the root classes](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#extending-the-root-classes)
- [Statements](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#statements)    
  - [Create tables](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#create-tables)
    - [Adding columns](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#adding-columns)
      - [Default values](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#default-values)
      - [Unique](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#unique)
      - [Not null](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#not-null)
      - [Primary keys](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#primary-keys)
      - [Autoincrement identities](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#autoincrement-identities)
      - [Column comments](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#column-comments)
    - [As copy](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#as-copy)    
  - [Select](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#select)
    - [Join](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#join)
    - [Order by](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#order-by)
  - [Update](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#update)
  - [Delete](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#Delete)
  - [Where](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#where)
  - [On more than](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#on-more-than)
  - [On less than](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#on-less-than)
  - [Insert](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#insert)
  - [Drop](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#drop)
  - [Alter](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#alter)
  - [On fail](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#on-fail)
- [Trigger events](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#trigger-events)  


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
  `DatabaseAccess` is the root class for all database classes. If you want to implement an entirely new system on how to handle the database access, then you should extend this class. This is only recommended if you really know what is going on inside the library. For most cases it will be sufficient to extend `EmbeddedDatabase` or `RemoteDatabase` as they already implement a fully functioning trigger system.


  #### EmbeddedDatabase or RemoteDatabase?
  The main difference between the two implementations is how they handle [triggers](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#trigger-events). The `EmbeddedDatabase` class will install the jar file of your program to the database, so that triggers will call the internal methods directly. The `RemoteDatabase` will work with a trigger table where new trigger messages are added and read from on a set interval. 
  If you can, always go for an `EmbeddedDatabase` implementation as triggers will be reported much faster which can increase your programs performance based on trigger usage.

Since there is no difference in extending one or the other, further examples will always use an `EmbeddedDatabase`.


#### Extending the root classes
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
See [this](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#create-tables) for more information on how to create tables.

So now that we have the [configuration](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#database-configuration) and the [Database implementation](https://github.com/Bowtie8904/BtDatabase/blob/master/README.md#databaseaccess-class) we can combine the two and create our first database.

```Java
DatabaseConfiguration config = new DatabaseConfiguration()
                                                            .path("./db") 
                                                            .create() 
                                                            .useUnicode() 
                                                            .characterEncoding("utf8") 
                                                            .autoReconnect(); 
Database db = new Database(config);
```
Calling the constructor will start the setup process of the database.
Once that is done, the database is fully functional and accessible.

## Statements

### Create tables
To create a new table simply call `create()` on your `DatabaseAccess` implementation.

The simplest creation statement would look like this:
```Java
db.create().table("testtable")
                .execute(true);
```
- call `create()`
- specify that you want to create a table and what the name should be
- call `execute`, passing `true` makes the statement print logs during execution

The `testtable` will have only one column, the `default_id` which is created automatically and contains a unique key for each entry. 
Of course tables can be more complex than that.

#### Adding columns
Custom column can be added like this:
```Java
db.create().table("testtable")
                .column("test_text", SqlType.VARCHAR).size(50).add()
                .column("test_long", SqlType.LONG).add()
                .execute(true);
```
- specify the name of the column
- specify the type of the column
- columns of type VARCHAR will need a specified size
- call `add` to finish the column configuration


#### Default values
Default values can be added to a column like this:
```Java
db.create().table("testtable")
                .column("test_bool", SqlType.BOOLEAN).defaultValue(false).add()
                .column("test_time", SqlType.TIME).defaultValue(SqlValue.CURRENT_TIME).add()
                .execute(true);
```


#### Unique
Columns can be marked as `unique` so that there can not be any duplicate values inside that column.
```Java
db.create().table("testtable")
                .column("test_long", SqlType.LONG).unique().add()
                .execute(true);
```


#### Not null
Columns can be marked as `not null` so that when inserting an entry there must be a value inside that column.
```Java
db.create().table("testtable")
                .column("test_long", SqlType.LONG).notNull.add()
                .execute(true);
```


#### Primary keys
Columns can be marked as `primary key` so that when inserting an entry there must be a unique value inside that column. 
```Java
db.create().table("testtable")
                .column("test_text", SqlType.VARCHAR).size(50).primaryKey().add()
                .execute(true);
```


#### Autoincrement identities
To avoid having the library add the `default_id` column automatically as an identity you can specify you own identity column.

Such columns need to follow this set of requirements:
- need to be of type LONG
- have to be set as identity with generation type `ALWAYS`

```Java
db.create().table("testtable")
                .column("test_id", SqlType.LONG).asIdentity(Generated.ALWAYS).autoIncrement(5).add()
                .execute(true);
```
By default the values will be autoincremented by 1, but you can override that value with your own desired incrementation by calling `autoIncrement`.


#### As Copy
You can create a table as a copy of a selected result set or another table.

To copy from a select statement:
```Java
db.create().table("testtable2")
                as(db.select()
                    .from("testtable"))
                .execute(true);
```
This will copy the selected data as well. If you only want the structure without any data call `withData(false)`:
```Java
db.create().table("testtable2")
                as(db.select()
                    .from("testtable"))
                .withData(false)    
                .execute(true);
```

To copy a table directly:
```Java
db.create().table("testtable2")
                .asCopyOf("testtable")
                .execute(true);
```
This will copy the selected data as well. If you only want the structure without any data call `withData(false)`:
```Java
db.create().table("testtable2")
                .asCopyOf("testtable")
                .withData(false)    
                .execute(true);
```  

### Select
To start a new select statement simply call `select` on your `DatabaseAccess` implementation. Calling `select` without parameters is equivalent to calling `select("*")`. 
```Java
db.select()
          .from("testtable")
          .execute(true);
                
db.select("*")
          .from("testtable")
          .execute(true);   
```

You can also specify the columns that you want to select.
```Java
db.select("test_text", "test_long)
                .from("testtable")
                .execute(true);
```


  #### Join
  
  **Inner/natural join**
  
  To create an inner or natural join simply call `join` on the select statement.
  For an inner join specify an `on` or `using` clause (explained below), for a natural join just call `natural()`.
  
  ```Java
  db.select().from("testtable")
                  .join("testtable2).using("test_text")
                  .execute(true);
                  
  db.select().from("testtable")
                  .join("testtable2).natural()
                  .execute(true);                
  ```
  
  **Left join**
  
  To create a left join call `leftJoin` on the select statement.
  
  ```Java
  db.select().from("testtable")
                  .leftJoin("testtable2).using("test_text")
                  .execute(true);
  ```                
                  
  
  **Right join**
  
  **On clause**
  
  **Using clause**
  
  
  #### Order by


### Insert


### Update


### Delete


### Where


### On more than


### On less than


### Drop


### Alter


### On fail


## Trigger events
