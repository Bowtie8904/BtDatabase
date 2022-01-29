package bt.db.store;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.exc.SqlEntryException;
import bt.db.statement.impl.InsertStatement;
import bt.db.statement.impl.UpdateStatement;
import bt.db.statement.result.SqlResult;
import bt.db.statement.result.SqlResultSet;
import bt.db.store.anot.*;
import bt.log.Log;
import bt.reflect.field.Fields;
import bt.types.SimpleTripple;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Offers an interface and static methods to persist and initialize objects who make use of the {@link Column},
 * {@link Table}, {@link Identity} and {@link NoPersist} annotations.
 *
 * @author &#8904
 */
public interface SqlEntry
{
    /**
     * Persists all important fields of this instance into the given database.
     *
     * @param db
     */
    public void persist(DatabaseAccess db);

    /**
     * Initializes all important fields of this instance.
     *
     * @param db
     */
    public void init(DatabaseAccess db);

    /**
     * Initializes an instance of the given class by using values from the given database that match the given identity.
     *
     * <p>
     * To allow the initialization of an instance, the class must meet the following requirements:
     *
     * <ul>
     * <li>the class must implement a constructor without arguments</li>
     * <li>all fields that should be initialized need a {@link Column} annotation</li>
     * <li>the class either needs a global {@link Table} annotation or one for every required field</li>
     * <li>one of the fields must also be marked with an {@link Identity} annotation</li>
     * <li>the identity field must be of type long</li>
     * </ul>
     * </p>
     *
     * @param db  The database to use to retrieve the column values.
     * @param cls The class to create an instance of.
     * @param id  The id to look for in the database.
     *
     * @return The instance or null if the database does not contain an entry with the given id.
     */
    public static <T> T init(DatabaseAccess db, Class<T> cls, long id)
    {
        // attempting to create a new instance of the given class by calling the empty constructor
        T entry = null;
        try
        {
            Constructor<T> construct = cls.getDeclaredConstructor();
            construct.setAccessible(true);
            entry = construct.newInstance();
        }
        catch (InstantiationException | IllegalAccessException
                | InvocationTargetException | SecurityException e1)
        {
            throw new SqlEntryException("Failed to create new instance", e1);
        }
        catch (NoSuchMethodException noEx)
        {
            throw new SqlEntryException("Class must implement a constructor without arguments.", noEx);
        }

        if (entry != null)
        {
            // searching for the identity field to prepare the instance for further initialization
            for (Field field : Fields.getAllFields(cls))
            {
                Column col = field.getAnnotation(Column.class);

                if (col != null)
                {
                    Identity ident = field.getAnnotation(Identity.class);

                    if (ident != null)
                    {
                        if (field.getType() != Long.TYPE)
                        {
                            throw new SqlEntryException("Identity field must of type long.");
                        }
                        else
                        {
                            field.setAccessible(true);
                            try
                            {
                                field.set(entry, id);
                                break;
                            }
                            catch (IllegalArgumentException | IllegalAccessException e)
                            {
                                throw new SqlEntryException("Failed to set value during initialization", e);
                            }
                        }
                    }
                }
            }

            entry = SqlEntry.init(db,
                                  entry);
        }

        return entry;
    }

    /**
     * Initializes instances of the given class by using rows from the table defined by the classes {@link Table}
     * annotation.
     *
     * <p>
     * To allow the initialization of an instance, the class must meet the following requirements:
     *
     * <ul>
     * <li>the class must implement a constructor without arguments</li>
     * <li>all fields that should be initialized need a {@link Column} annotation</li>
     * <li>the class either needs a global {@link Table} annotation or one for every required field</li>
     * <li>one of the fields must also be marked with an {@link Identity} annotation</li>
     * <li>the identity field must be of type long</li>
     * </ul>
     * </p>
     *
     * @param db  The database to use to retrieve the column values.
     * @param cls The class to create instances of.
     *
     * @return A list filled with one instance for each row contained in the defined table.
     */
    public static <T> List<T> init(DatabaseAccess db, Class<T> cls)
    {
        return SqlEntry.init(db,
                             cls,
                             null);
    }

    /**
     * Initializes instances of the given class by using matching database identities with the given IDs.
     *
     * <p>
     * To allow the initialization of an instance, the class must meet the following requirements:
     *
     * <ul>
     * <li>the class must implement a constructor without arguments</li>
     * <li>all fields that should be initialized need a {@link Column} annotation</li>
     * <li>the class either needs a global {@link Table} annotation or one for every required field</li>
     * <li>one of the fields must also be marked with an {@link Identity} annotation</li>
     * <li>the identity field must be of type long</li>
     * </ul>
     * </p>
     *
     * @param db  The database to use to retrieve the column values.
     * @param cls The class to create instances of.
     * @param ids A list of IDs which match the identities of the instances that should be initialized.
     *
     * @return A list filled with one instance for each row contained in the defined table.
     */
    public static <T> List<T> init(DatabaseAccess db, Class<T> cls, List<Long> ids)
    {
        // just used to either get all entries or the ones with an id in the given list
        long checkValue = ids == null ? Long.MIN_VALUE : Long.MAX_VALUE;

        if (ids == null)
        {
            ids = List.of(Long.MIN_VALUE);
        }

        List<T> instances = new ArrayList<>();

        Table globalTable = cls.getAnnotation(Table.class);
        Table mainTable = null;
        List<String> tables = new ArrayList<>();

        if (globalTable != null)
        {
            tables.add(globalTable.value().toLowerCase());
        }

        String idField = null;

        for (Field field : Fields.getAllFields(cls))
        {
            Table table = field.getAnnotation(Table.class);

            if (table != null && !tables.contains(table.value().toLowerCase()))
            {
                tables.add(table.value().toLowerCase());
            }

            Identity ident = field.getAnnotation(Identity.class);

            if (ident != null && idField == null)
            {
                if (field.getType() != Long.TYPE)
                {
                    throw new SqlEntryException(
                            "Identity field must of type long.");
                }

                Column col = field.getAnnotation(Column.class);

                if (col != null)
                {
                    idField = col.name();

                    if (table != null)
                    {
                        mainTable = table;
                    }
                    else if (globalTable != null)
                    {
                        mainTable = globalTable;
                    }
                    else
                    {
                        throw new SqlEntryException(
                                "Class needs either a global table annotation or a table annotation on every persistance field.");
                    }
                }
            }
        }

        if (idField == null)
        {
            throw new SqlEntryException(
                    "Class requires a valid identity field of type long.");
        }

        Map<Long, T> entries = new HashMap<>();

        SqlResultSet masterSet = db.select(idField)
                                   .from(mainTable.value())
                                   .where(idField)
                                   .in(ids.toArray())
                                   .or(idField)
                                   .greaterOrEqual(checkValue)
                                   .onLessThan(1,
                                               (i, sqlSet) ->
                                               {
                                                   return sqlSet;
                                               })
                                   .execute();

        for (SqlResult result : masterSet)
        {
            T entry = null;
            try
            {
                Constructor<T> construct = cls.getDeclaredConstructor();
                construct.setAccessible(true);
                entry = construct.newInstance();
            }
            catch (InstantiationException | IllegalAccessException
                    | InvocationTargetException | SecurityException e1)
            {
                throw new SqlEntryException("Failed to create new instance", e1);
            }
            catch (NoSuchMethodException noEx)
            {
                throw new SqlEntryException(
                        "Class must implement a constructor without arguments.", noEx);
            }

            if (entry != null)
            {
                entries.put(result.getLong(idField),
                            entry);
            }
        }

        long id = -1;
        for (String table : tables)
        {
            SqlResultSet set = db.select()
                                 .from(table)
                                 .where(idField)
                                 .in(ids.toArray())
                                 .or(idField)
                                 .greaterOrEqual(checkValue)
                                 .onLessThan(1,
                                             (i, sqlSet) ->
                                             {
                                                 return sqlSet;
                                             })
                                 .execute();

            for (SqlResult result : set)
            {
                id = result.getLong(idField);
                T entry = entries.get(id);

                if (entry != null)
                {
                    for (Field field : Fields.getAllFields(cls))
                    {
                        Column col = field.getAnnotation(Column.class);
                        String tableName = null;

                        Table tableAn = field.getAnnotation(Table.class);
                        SqlEntryField sqlEntryField = field.getAnnotation(SqlEntryField.class);

                        if (sqlEntryField != null)
                        {
                            Object value = SqlEntry.init(db,
                                                         field.getType(),
                                                         id);

                            field.setAccessible(true);
                            try
                            {
                                field.set(entry,
                                          value);
                            }
                            catch (IllegalAccessException e)
                            {
                                throw new SqlEntryException("Failed to set value", e);
                            }
                            continue;
                        }

                        if (col == null)
                        {
                            continue;
                        }

                        if (tableAn != null)
                        {
                            tableName = tableAn.value();
                        }
                        else if (globalTable != null)
                        {
                            tableName = globalTable.value();
                        }
                        else
                        {
                            throw new SqlEntryException(
                                    "Class needs either a global table annotation or a table annotation on every persistance field.");
                        }

                        if (col != null && tableName != null && tableName.equalsIgnoreCase(table))
                        {
                            SqlType type = col.type();
                            String name = col.name();
                            Object value = result.get(name);

                            field.setAccessible(true);
                            try
                            {
                                field.set(entry,
                                          value);
                            }
                            catch (IllegalAccessException e)
                            {
                                throw new SqlEntryException("Failed to set value", e);
                            }
                        }
                    }
                }
            }
        }

        instances.addAll(entries.values());

        return instances;
    }

    /**
     * Initializes the given instance by using values from the given database that match the value of the
     * {@link Identity} field.
     *
     * <p>
     * To allow the initialization of an instance, the class must meet the following requirements:
     *
     * <ul>
     * <li>the class must implement a constructor without arguments</li>
     * <li>all fields that should be initialized need a {@link Column} annotation</li>
     * <li>the class either needs a global {@link Table} annotation or one for every required field</li>
     * <li>one of the fields must also be marked with an {@link Identity} annotation</li>
     * <li>the identity field must be of type long</li>
     * </ul>
     * </p>
     *
     * @param db    The database to use to retrieve the column values.
     * @param entry The instance to initialize.
     *
     * @return The initialized instance or null if the database did not contain data for the set identity.
     */
    public static <T> T init(DatabaseAccess db, T entry)
    {
        Map<String, List<SimpleTripple<String, SqlType, Object>>> tableValues = new HashMap<>();

        Table globalTable = entry.getClass().getAnnotation(Table.class);
        List<String> tables = new ArrayList<>();

        if (globalTable != null)
        {
            tables.add(globalTable.value().toLowerCase());
        }

        SimpleTripple<String, SqlType, Long> id = null;

        List<Field> sqlEntryFields = new ArrayList<>();

        for (Field field : Fields.getAllFields(entry.getClass()))
        {
            Column col = field.getAnnotation(Column.class);
            Table table = field.getAnnotation(Table.class);
            SqlEntryField sqlEntryField = field.getAnnotation(SqlEntryField.class);

            if (sqlEntryField != null)
            {
                sqlEntryFields.add(field);
            }

            if (table != null && !tables.contains(table.value().toLowerCase()))
            {
                tables.add(table.value().toLowerCase());
            }

            if (col != null)
            {
                Identity ident = field.getAnnotation(Identity.class);

                if (ident != null)
                {
                    SqlType type = col.type();
                    String name = col.name();
                    Object value = null;

                    try
                    {
                        field.setAccessible(true);
                        value = field.get(entry);
                    }
                    catch (IllegalAccessException e)
                    {
                        throw new SqlEntryException("Failed to get field value", e);
                    }

                    if (id == null)
                    {
                        if (value == null)
                        {
                            throw new SqlEntryException(
                                    "Identity field can't be null.");
                        }
                        else if (field.getType() != Long.TYPE)
                        {
                            throw new SqlEntryException(
                                    "Identity field must of type long.");
                        }
                        else
                        {
                            id = new SimpleTripple<>(name,
                                                     type,
                                                     (long)value);
                        }
                    }
                    else
                    {
                        throw new SqlEntryException(
                                "Multiple annotated Identity fields can't be initialized.");
                    }
                }
            }
        }

        if (id == null)
        {
            throw new SqlEntryException(
                    "Class without Identity annotation can't be automatically initialized.");
        }

        for (Field field : sqlEntryFields)
        {
            Object value = SqlEntry.init(db,
                                         field.getType(),
                                         id.getSecondValue().longValue());

            field.setAccessible(true);
            try
            {
                field.set(entry,
                          value);
            }
            catch (IllegalAccessException e)
            {
                throw new SqlEntryException("Failed to set value", e);
            }
        }

        for (String table : tables)
        {
            SqlResultSet set = db.select()
                                 .from(table)
                                 .where(id.getKey())
                                 .equal(id.getSecondValue().longValue())
                                 .onLessThan(1,
                                             (i, sqlSet) ->
                                             {
                                                 return sqlSet;
                                             })
                                 .execute();

            if (set.size() > 1)
            {
                throw new SqlEntryException(
                        "Multiple results for identity = " + id.getSecondValue().longValue()
                                + ". Identity field must be unique for automated initialization.");
            }

            if (set.size() != 0)
            {
                SqlResult result = set.get(0);

                for (Field field : Fields.getAllFields(entry.getClass()))
                {
                    Column col = field.getAnnotation(Column.class);
                    Identity ident = field.getAnnotation(Identity.class);
                    Table tableAn = field.getAnnotation(Table.class);

                    if (col == null)
                    {
                        continue;
                    }

                    String usedTable = null;

                    if (tableAn == null)
                    {
                        if (globalTable != null)
                        {
                            usedTable = globalTable.value();
                        }
                    }
                    else
                    {
                        usedTable = tableAn.value();
                    }

                    if (usedTable == null)
                    {
                        throw new SqlEntryException(
                                "Class needs either a global table annotation or a table annotation on every persistance field.");
                    }

                    if (col != null && ident == null && usedTable.equalsIgnoreCase(table))
                    {
                        SqlType type = col.type();
                        String name = col.name();
                        Object value = result.get(name);

                        field.setAccessible(true);
                        try
                        {
                            field.set(entry,
                                      value);
                        }
                        catch (IllegalAccessException e)
                        {
                            throw new SqlEntryException("Failed to set value", e);
                        }
                    }
                }
            }
            else
            {
                return null;
            }
        }

        return entry;
    }

    /**
     * Persists the given instance to the database by using the {@link Table} and {@link Column} annotation of the
     * class.
     *
     * <p>
     * To allow the persistance of an instance, the class must meet the following requirements:
     *
     * <ul>
     * <li>the class must implement a constructor without arguments</li>
     * <li>all fields that should be persisted need a {@link Column} annotation</li>
     * <li>the class either needs a global {@link Table} annotation or one for every required field</li>
     * <li>one of the fields must also be marked with an {@link Identity} annotation</li>
     * <li>the identity field must be of type long</li>
     * </ul>
     * </p>
     *
     * @param db    The database to use for persisting.
     * @param entry The instance to persist.
     */
    public static <T> void persist(DatabaseAccess db, T entry)
    {
        Map<String, List<SimpleTripple<String, SqlType, Object>>> tableValues = new HashMap<>();
        List<Object> sqlEntryFields = new ArrayList<>();

        Table globalTable = entry.getClass().getAnnotation(Table.class);
        boolean hasGlobalTable = globalTable != null;

        if (hasGlobalTable)
        {
            tableValues.put(globalTable.value().toUpperCase(),
                            new ArrayList<>());
        }

        boolean hasValues = false;
        boolean persistId = true;

        SimpleEntry<String, Long> id = null;

        for (Field field : Fields.getAllFields(entry.getClass()))
        {
            Table table = field.getAnnotation(Table.class);
            Column col = field.getAnnotation(Column.class);
            NoPersist pers = field.getAnnotation(NoPersist.class);
            Identity ident = field.getAnnotation(Identity.class);
            SqlEntryField sqlEntryField = field.getAnnotation(SqlEntryField.class);

            Object value = null;

            try
            {
                field.setAccessible(true);
                value = field.get(entry);
            }
            catch (IllegalAccessException e)
            {
                throw new SqlEntryException("Failed to get value", e);
            }

            if (pers == null && sqlEntryField != null && value != null)
            {
                sqlEntryFields.add(value);
                hasValues = true;
                continue;
            }

            if (col == null)
            {
                continue;
            }

            SqlType type = col.type();
            String name = col.name();

            if (col != null && pers == null && ident == null)
            {
                if (table != null)
                {
                    List<SimpleTripple<String, SqlType, Object>> values = tableValues.get(table.value().toUpperCase());

                    if (values == null)
                    {
                        values = new ArrayList<>();
                        tableValues.put(table.value().toUpperCase(),
                                        values);
                    }

                    values.add(new SimpleTripple(name,
                                                 type,
                                                 value));
                    hasValues = true;
                }
                else if (hasGlobalTable)
                {
                    List<SimpleTripple<String, SqlType, Object>> values = tableValues
                            .get(globalTable.value()
                                            .toUpperCase());

                    values.add(new SimpleTripple(name,
                                                 type,
                                                 value));
                    hasValues = true;
                }
                else
                {
                    throw new SqlEntryException(
                            "Class needs either a global table annotation or a table annotation on every persistance field.");
                }

            }

            if (ident != null)
            {
                persistId = pers == null;

                if (id == null)
                {
                    if (value == null)
                    {
                        throw new SqlEntryException(
                                "Identity field can't be null.");
                    }
                    else if (field.getType() != Long.TYPE)
                    {
                        throw new SqlEntryException(
                                "Identity field must of type long.");
                    }
                    else
                    {
                        id = new SimpleEntry<>(name,
                                               (long)value);
                    }
                }
                else
                {
                    throw new SqlEntryException(
                            "Multiple annotated Identity fields can't be persisted.");
                }
            }
        }

        if (!hasValues)
        {
            throw new SqlEntryException(
                    "Class needs to have at least one non identity value to persist.");
        }

        for (String tableName : tableValues.keySet())
        {
            InsertStatement insert = db.insert().into(tableName);
            UpdateStatement update = db.update(tableName);

            for (SimpleTripple<String, SqlType, Object> value : tableValues.get(tableName))
            {
                if (value.getSecondValue() == null)
                {
                    insert.setNull(value.getKey(),
                                   value.getFirstValue());
                    update.setNull(value.getKey(),
                                   value.getFirstValue());
                }
                else
                {
                    insert.set(value.getKey(),
                               value.getSecondValue(),
                               value.getFirstValue());
                    update.set(value.getKey(),
                               value.getSecondValue(),
                               value.getFirstValue());
                }
            }

            if (id == null)
            {
                throw new SqlEntryException(
                        "Class without Identity annotation can't be automatically persisted.");
            }

            if (persistId)
            {
                insert.set(id.getKey(),
                           id.getValue(),
                           SqlType.LONG);
                update.set(id.getKey(),
                           id.getValue(),
                           SqlType.LONG);
            }

            insert.commit();
            insert.onFail((select, e) ->
                          {
                              Log.error("Failed to persist entry", e);
                              db.rollback();
                              return -1;
                          });

            update.commit();
            update.onFail((select, e) ->
                          {
                              db.rollback();
                              return -1;
                          });

            update.where(id.getKey()).equal(id.getValue().longValue());

            db.select(id.getKey())
              .from(tableName)
              .where(id.getKey())
              .equal(id.getValue().longValue())
              .onLessThan(1,
                          insert)
              .onMoreThan(0,
                          update)
              .onFail((select, e) ->
                      {
                          db.rollback();
                          return null;
                      })
              .execute();
        }

        for (Object obj : sqlEntryFields)
        {
            if (obj instanceof SqlEntry)
            {
                ((SqlEntry)obj).persist(db);
            }
            else
            {
                SqlEntry.persist(db,
                                 obj);
            }
        }
    }
}