/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.repo.jdbc;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import static java.sql.Types.*;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.cojen.classfile.TypeDesc;
import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.MismatchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableKey;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.info.StorablePropertyConstraint;

/**
 * Provides additional metadata for a {@link Storable} type needed by
 * JDBCRepository. The storable type must match to a table in an external
 * database. All examined data is cached, so repeat examinations are fast,
 * unless the examination failed.
 *
 * @author Brian S O'Neill
 * @author Adam D Bradley
 * @author Tobias Holgers
 */
public class JDBCStorableIntrospector extends StorableIntrospector {
    // Maps compound keys to softly referenced JDBCStorableInfo objects.
    @SuppressWarnings("unchecked")
    private static Map<Object, JDBCStorableInfo<?>> cCache = new SoftValuedHashMap();

    /**
     * Examines the given class and returns a JDBCStorableInfo describing it. A
     * MalformedTypeException is thrown for a variety of reasons if the given
     * class is not a well-defined Storable type or if it can't match up with
     * an entity in the external database.
     *
     * @param type Storable type to examine
     * @param ds source of JDBC connections to use for matching to a table
     * @param catalog optional catalog to search
     * @param schema optional schema to search
     * @throws MalformedTypeException if Storable type is not well-formed
     * @throws RepositoryException if there was a problem in accessing the database
     * @throws IllegalArgumentException if type is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> JDBCStorableInfo<S> examine
        (Class<S> type, DataSource ds, String catalog, String schema)
        throws SQLException, SupportException
    {
        return examine(type, ds, catalog, schema, null, false);
    }

    static <S extends Storable> JDBCStorableInfo<S> examine
        (Class<S> type, DataSource ds, String catalog, String schema, SchemaResolver resolver, boolean primaryKeyCheckDisabled)
        throws SQLException, SupportException
    {
        Object key = KeyFactory.createKey(new Object[] {type, ds, catalog, schema});

        synchronized (cCache) {
            JDBCStorableInfo<S> jInfo = (JDBCStorableInfo<S>) cCache.get(key);
            if (jInfo != null) {
                return jInfo;
            }

            // Call superclass for most info.
            StorableInfo<S> mainInfo = examine(type);
            Connection con = ds.getConnection();
            try {
                try {
                    jInfo = examine(mainInfo, con, catalog, schema, resolver, primaryKeyCheckDisabled);
                    if (!jInfo.isSupported() && resolver != null &&
                        resolver.resolve(mainInfo, con, catalog, schema))
                    {
                        jInfo = examine(mainInfo, con, catalog, schema, resolver, primaryKeyCheckDisabled);
                    }
                } catch (SupportException e) {
                    if (resolver != null && resolver.resolve(mainInfo, con, catalog, schema)) {
                        jInfo = examine(mainInfo, con, catalog, schema, resolver, primaryKeyCheckDisabled);
                    } else {
                        throw e;
                    }
                }
            } finally {
                try {
                    con.close();
                } catch (SQLException e) {
                    // Don't care.
                }
            }

            cCache.put(key, jInfo);

            // Finish resolving join properties, after properties have been
            // added to cache. This makes it possible for joins to (directly or
            // indirectly) reference their own enclosing type.
            for (JDBCStorableProperty<S> jProperty : jInfo.getAllProperties().values()) {
                ((JProperty<S>) jProperty).fillInternalJoinElements(ds, catalog, schema, resolver);
                ((JProperty<S>) jProperty).fillExternalJoinElements(ds, catalog, schema, resolver);
            }

            return jInfo;
        }
    }

    /**
     * Uses the given database connection to query database metadata. This is
     * used to bind storables to tables, and properties to columns. Other
     * checks are performed to ensure that storable type matches well with the
     * definition in the database.
     */
    private static <S extends Storable> JDBCStorableInfo<S> examine
        (StorableInfo<S> mainInfo, Connection con,
         final String searchCatalog, final String searchSchema,
         SchemaResolver resolver, boolean primaryKeyCheckDisabled)
        throws SQLException, SupportException
    {
        final DatabaseMetaData meta = con.getMetaData();

        final String databaseProductName = meta.getDatabaseProductName();
        final String userName = meta.getUserName();

        String[] tableAliases;
        if (mainInfo.getAliasCount() > 0) {
            tableAliases = mainInfo.getAliases();
        } else {
            String name = mainInfo.getStorableType().getSimpleName();
            tableAliases = generateAliases(name);
        }

        // Try to find matching table from aliases.
        String catalog = null, schema = null, tableName = null, tableType = null;
        findName: {
            // The call to getTables may return several matching tables. This
            // map defines the "best" table type we'd like to use. The higher
            // the number the better.
            Map<String, Integer> fitnessMap = new HashMap<String, Integer>();
            fitnessMap.put("LOCAL TEMPORARY", 1);
            fitnessMap.put("GLOBAL TEMPORARY", 2);
            fitnessMap.put("VIEW", 3);
            fitnessMap.put("SYSTEM TABLE", 4);
            fitnessMap.put("TABLE", 5);
            fitnessMap.put("ALIAS", 6);
            fitnessMap.put("SYNONYM", 7);

            for (int i=0; i<tableAliases.length; i++) {
                ResultSet rs = meta.getTables(searchCatalog, searchSchema, tableAliases[i], null);
                try {
                    int bestFitness = 0;
                    while (rs.next()) {
                        String type = rs.getString("TABLE_TYPE");
                        Integer fitness = fitnessMap.get(type);
                        if (fitness != null) {
                            String rsSchema = rs.getString("TABLE_SCHEM");

                            if (searchSchema == null) {
                                if (userName != null && userName.equalsIgnoreCase(rsSchema)) {
                                    // Favor entities whose schema name matches
                                    // the user name.
                                    fitness += 7;
                                }
                            }

                            if (fitness > bestFitness) {
                                bestFitness = fitness;
                                catalog = rs.getString("TABLE_CAT");
                                schema = rsSchema;
                                tableName = rs.getString("TABLE_NAME");
                                tableType = type;
                            }
                        }
                    }
                } finally {
                    rs.close();
                }

                if (tableName != null) {
                    // Found a match, so stop checking aliases.
                    break;
                }
            }
        }

        if (tableName == null && !mainInfo.isIndependent()) {
            StringBuilder buf = new StringBuilder();
            buf.append("Unable to find matching table name for type \"");
            buf.append(mainInfo.getStorableType().getName());
            buf.append("\" by looking for ");
            appendToSentence(buf, tableAliases);
            buf.append(" with catalog " + searchCatalog + " and schema " + searchSchema);
            throw new MismatchException(buf.toString());
        }

        String qualifiedTableName = tableName;
        String resolvedTableName = tableName;

        // Oracle specific stuff...
        // TODO: Migrate this to OracleSupportStrategy.
        if (tableName != null && databaseProductName.toUpperCase().contains("ORACLE")) {
            if ("TABLE".equals(tableType) && searchSchema != null) {
                // Qualified table name references the schema. Used by SQL statements.
                qualifiedTableName = searchSchema + '.' + tableName;
            } else if ("SYNONYM".equals(tableType)) {
                // Try to get the real schema. This call is Oracle specific, however.
                String select = "SELECT TABLE_OWNER,TABLE_NAME " +
                    "FROM ALL_SYNONYMS " +
                    "WHERE OWNER=? AND SYNONYM_NAME=?";
                PreparedStatement ps = con.prepareStatement(select);
                ps.setString(1, schema); // in Oracle, schema is the owner
                ps.setString(2, tableName);
                try {
                    ResultSet rs = ps.executeQuery();
                    try {
                        if (rs.next()) {
                            schema = rs.getString("TABLE_OWNER");
                            resolvedTableName = rs.getString("TABLE_NAME");
                        }
                    } finally {
                        rs.close();
                    }
                } finally {
                    ps.close();
                }
            }
        }

        // Gather information on all columns such that metadata only needs to
        // be retrieved once.
        Map<String, ColumnInfo> columnMap =
            new TreeMap<String, ColumnInfo>(String.CASE_INSENSITIVE_ORDER);

        if (resolvedTableName != null) {
            ResultSet rs = meta.getColumns(catalog, schema, resolvedTableName, null);
            rs.setFetchSize(1000);
            try {
                while (rs.next()) {
                    ColumnInfo info = new ColumnInfo(rs);
                    columnMap.put(info.columnName, info);
                }
            } finally {
                rs.close();
            }
        }

        // Make sure that all properties have a corresponding column.
        Map<String, ? extends StorableProperty<S>> mainProperties = mainInfo.getAllProperties();
        Map<String, String> columnToProperty = new HashMap<String, String>();
        Map<String, JDBCStorableProperty<S>> jProperties =
            new LinkedHashMap<String, JDBCStorableProperty<S>>(mainProperties.size());

        ArrayList<String> errorMessages = new ArrayList<String>();

        for (StorableProperty<S> mainProperty : mainProperties.values()) {
            if (mainProperty.isDerived() || mainProperty.isJoin() || tableName == null) {
                jProperties.put(mainProperty.getName(), new JProperty<S>(mainProperty, primaryKeyCheckDisabled));
                continue;
            }

            String[] columnAliases;
            if (mainProperty.getAliasCount() > 0) {
                columnAliases = mainProperty.getAliases();
            } else {
                columnAliases = generateAliases(mainProperty.getName());
            }

            JDBCStorableProperty<S> jProperty = null;
            boolean addedError = false;

            findName: for (int i=0; i<columnAliases.length; i++) {
                ColumnInfo columnInfo = columnMap.get(columnAliases[i]);
                if (columnInfo != null) {
                    AccessInfo accessInfo = getAccessInfo
                        (mainProperty,
                         columnInfo.dataType, columnInfo.dataTypeName,
                         columnInfo.columnSize, columnInfo.decimalDigits);

                    if (accessInfo == null) {
                        TypeDesc propertyType = TypeDesc.forClass(mainProperty.getType());
                        String message =
                            "Property \"" + mainProperty.getName() +
                            "\" has type \"" + propertyType.getFullName() +
                            "\" which is incompatible with database type \"" +
                            columnInfo.dataTypeName + '"';

                        if (columnInfo.decimalDigits > 0) {
                            message += " (decimal digits = " + columnInfo.decimalDigits + ')';
                        }

                        errorMessages.add(message);
                        addedError = true;
                        break findName;
                    }

                    if (columnInfo.nullable) {
                        if (!mainProperty.isNullable()) {
                            errorMessages.add
                                ("Property \"" + mainProperty.getName() +
                                 "\" must have a Nullable annotation");
                        }
                    } else {
                        if (mainProperty.isNullable() && !mainProperty.isIndependent()) {
                            errorMessages.add
                                ("Property \"" + mainProperty.getName() +
                                 "\" must not have a Nullable annotation");
                        }
                    }

                    boolean autoIncrement = mainProperty.isAutomatic();
                    if (autoIncrement) {
                        // Need to execute a little query to check if column is
                        // auto-increment or not. This information is not available in
                        // the regular database metadata prior to jdk1.6.

                        PreparedStatement ps = con.prepareStatement
                            ("SELECT " + columnInfo.columnName +
                             " FROM " + tableName +
                             " WHERE 1=0");

                        try {
                            ResultSet rs = ps.executeQuery();
                            try {
                                autoIncrement = rs.getMetaData().isAutoIncrement(1);
                            } finally {
                                rs.close();
                            }
                        } finally {
                            ps.close();
                        }
                    }

                    jProperty = new JProperty<S>(mainProperty, columnInfo,
                                                 autoIncrement, primaryKeyCheckDisabled,
                                                 accessInfo.mResultSetGet,
                                                 accessInfo.mPreparedStatementSet,
                                                 accessInfo.getAdapter());

                    break findName;
                }
            }

            if (jProperty != null) {
                jProperties.put(mainProperty.getName(), jProperty);
                columnToProperty.put(jProperty.getColumnName(), jProperty.getName());
            } else {
                if (mainProperty.isIndependent()) {
                    jProperties.put(mainProperty.getName(), new JProperty<S>(mainProperty, primaryKeyCheckDisabled));
                } else if (!addedError) {
                    StringBuilder buf = new StringBuilder();
                    buf.append("Unable to find matching database column for property \"");
                    buf.append(mainProperty.getName());
                    buf.append("\" by looking for ");
                    appendToSentence(buf, columnAliases);
                    errorMessages.add(buf.toString());
                }
            }
        }

        if (errorMessages.size() > 0) {
            throw new MismatchException(mainInfo.getStorableType(), errorMessages);
        }

        // Now verify that primary or alternate keys match.

        if (resolvedTableName != null) checkPrimaryKey: {
            ResultSet rs;
            try {
                rs = meta.getPrimaryKeys(catalog, schema, resolvedTableName);
            } catch (SQLException e) {
                getLog().info
                    ("Unable to get primary keys for table \"" + resolvedTableName +
                     "\" with catalog " + catalog + " and schema " + schema + ": " + e);
                break checkPrimaryKey;
            }

            List<String> pkProps = new ArrayList<String>();

            try {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String propertyName = columnToProperty.get(columnName);

                    if (propertyName == null) {
                        errorMessages.add
                            ("Column \"" + columnName +
                             "\" must be part of primary or alternate key");
                        continue;
                    }

                    pkProps.add(propertyName);
                }
            } finally {
                rs.close();
            }

            if (errorMessages.size() > 0) {
                // Skip any extra checks.
                break checkPrimaryKey;
            }

            if (pkProps.size() == 0) {
                // If no primary keys are reported, don't even bother checking.
                // There's no consistent way to get primary keys, and entities
                // like views and synonyms don't usually report primary keys.
                // A primary key might even be logically defined as a unique
                // constraint.
                break checkPrimaryKey;
            }

            if (matchesKey(pkProps, mainInfo.getPrimaryKey())) {
                // Good. Primary key in database is same as in Storable.
                break checkPrimaryKey;
            }

            // Check if Storable has an alternate key which matches the
            // database's primary key.
            boolean foundAnyAltKey = false;
            for (StorableKey<S> altKey : mainInfo.getAlternateKeys()) {
                if (matchesKey(pkProps, altKey)) {
                    // Okay. Primary key in database matches a Storable
                    // alternate key.
                    foundAnyAltKey = true;

                    // Also check that declared primary key is a strict subset
                    // of the alternate key. If not, keep checking alt keys.

                    if (matchesSubKey(pkProps, mainInfo.getPrimaryKey())) {
                        break checkPrimaryKey;
                    }
                }
            }

            if (foundAnyAltKey) {
                errorMessages.add("Actual primary key matches a declared alternate key, " +
                                  "but declared primary key must be a strict subset. " +
                                  mainInfo.getPrimaryKey().getProperties() +
                                  " is not a subset of " + pkProps);
            } else {
                errorMessages.add("Actual primary key does not match any " +
                                  "declared primary or alternate key: " + pkProps);
            }
        }

        if (errorMessages.size() > 0) {
            if (primaryKeyCheckDisabled) {
                for (String errorMessage:errorMessages) {
                    getLog().warn("Suppressed error: " + errorMessage);
                }
                errorMessages.clear();
            } else {
                throw new MismatchException(mainInfo.getStorableType(), errorMessages);
            }
        }

        // IndexInfo is empty, as querying for it tends to cause a table analyze to run.
        IndexInfo[] indexInfo = new IndexInfo[0];

        if (needsQuotes(tableName)) {
            String quote = meta.getIdentifierQuoteString();
            if (quote != null && !quote.equals(" ")) {
                tableName = quote + tableName + quote;
                qualifiedTableName = quote + qualifiedTableName + quote;
            }
        }

        return new JInfo<S>
            (mainInfo, catalog, schema, tableName, qualifiedTableName, indexInfo, jProperties);
    }

    private static boolean needsQuotes(String str) {
        if (str == null) {
            return false;
        }
        if (str.length() == 0) {
            return true;
        }
        char c = str.charAt(0);
        if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_')) {
            return true;
        }
        for (int i=str.length(); --i>=0; ) {
            c = str.charAt(i);
            if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' ||
                  c >= '0' && c <= '9'))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesKey(Collection<String> keyProps, StorableKey<?> declaredKey) {
        if (keyProps.size() != declaredKey.getProperties().size()) {
            return false;
        }
        return matchesSubKey(keyProps, declaredKey);
    }

    /**
     * @return true if declared key properties are all found in the given keyProps set
     */
    private static boolean matchesSubKey(Collection<String> keyProps, StorableKey<?> declaredKey) {
        for (OrderedProperty<?> declaredKeyProp : declaredKey.getProperties()) {
            ChainedProperty<?> chained = declaredKeyProp.getChainedProperty();
            if (chained.getChainCount() > 0) {
                return false;
            }
            if (!keyProps.contains(chained.getLastProperty().getName())) {
                return false;
            }
        }
        return true;
    }

    private static Log getLog() {
        return LogFactory.getLog(JDBCStorableIntrospector.class);
    }

    /**
     * Figures out how to best access the given property, or returns null if
     * not supported. An adapter may be applied.
     *
     * @return null if not supported
     */
    private static AccessInfo getAccessInfo
        (StorableProperty property,
         int dataType, String dataTypeName, int columnSize, int decimalDigits)
    {
        AccessInfo info = getAccessInfo
            (property.getType(), dataType, dataTypeName, columnSize, decimalDigits);
        if (info != null) {
            return info;
        }

        // See if an appropriate adapter exists.
        StorablePropertyAdapter adapter = property.getAdapter();
        if (adapter != null) {
            Method[] toMethods = adapter.findAdaptMethodsTo(property.getType());
            for (Method toMethod : toMethods) {
                Class fromType = toMethod.getParameterTypes()[0];
                // Verify that reverse adapt method exists as well...
                if (adapter.findAdaptMethod(property.getType(), fromType) != null) {
                    // ...and try to get access info for fromType.
                    info = getAccessInfo
                        (fromType, dataType, dataTypeName, columnSize, decimalDigits);
                    if (info != null) {
                        info.setAdapter(adapter);
                        return info;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Figures out how to best access the given property, or returns null if
     * not supported. An adapter is not applied.
     *
     * @return null if not supported
     */
    private static AccessInfo getAccessInfo
        (Class desiredClass,
         int dataType, String dataTypeName, int columnSize, int decimalDigits)
    {
        if (!desiredClass.isPrimitive()) {
            TypeDesc desiredType = TypeDesc.forClass(desiredClass);
            if (desiredType.toPrimitiveType() != null) {
                desiredType = desiredType.toPrimitiveType();
                desiredClass = desiredType.toClass();
            }
        }

        if (desiredClass == Object.class) {
            return new AccessInfo("Object", Object.class);
        }

        Class actualClass;
        String suffix;

        switch (dataType) {
        default:
            return null;

        case BIT:
        case BOOLEAN:
            if (desiredClass == boolean.class) {
                actualClass = boolean.class;
                suffix = "Boolean";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case TINYINT:
            if (desiredClass == byte.class) {
                actualClass = byte.class;
                suffix = "Byte";
            } else if (desiredClass == short.class) {
                actualClass = short.class;
                suffix = "Short";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case SMALLINT:
            if (desiredClass == short.class) {
                actualClass = short.class;
                suffix = "Short";
            } else if (desiredClass == int.class) {
                actualClass = int.class;
                suffix = "Int";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case INTEGER:
            if (desiredClass == int.class) {
                actualClass = int.class;
                suffix = "Int";
            } else if (desiredClass == long.class) {
                actualClass = long.class;
                suffix = "Long";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case BIGINT:
            if (desiredClass == long.class) {
                actualClass = long.class;
                suffix = "Long";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case REAL:
        case FLOAT:
        case DOUBLE:
            if (desiredClass == float.class) {
                actualClass = float.class;
                suffix = "Float";
            } else if (desiredClass == double.class) {
                actualClass = double.class;
                suffix = "Double";
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case NUMERIC:
        case DECIMAL:
            if (desiredClass == int.class) {
                if (decimalDigits == 0) {
                    actualClass = int.class;
                    suffix = "Int";
                } else {
                    return null;
                }
            } else if (desiredClass == long.class) {
                if (decimalDigits == 0) {
                    actualClass = long.class;
                    suffix = "Long";
                } else {
                    return null;
                }
            } else if (desiredClass == double.class) {
                actualClass = double.class;
                suffix = "Double";
            } else if (desiredClass == BigDecimal.class) {
                actualClass = BigDecimal.class;
                suffix = "BigDecimal";
            } else if (desiredClass == short.class) {
                if (decimalDigits == 0) {
                    actualClass = short.class;
                    suffix = "Short";
                } else {
                    return null;
                }
            } else if (desiredClass == byte.class) {
                if (decimalDigits == 0) {
                    actualClass = byte.class;
                    suffix = "Byte";
                } else {
                    return null;
                }
            } else if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case CHAR:
        case VARCHAR:
        case LONGVARCHAR:
            if (desiredClass == String.class) {
                actualClass = String.class;
                suffix = "String";
            } else if (desiredClass == char.class && columnSize == 1) {
                actualClass = String.class;
                suffix = "String";
            } else {
                return null;
            }
            break;

        case DATE:
            // Treat Date as a Timestamp since some databases make no
            // distinction.  The DateTimeAdapter can be used to provide
            // more control over the desired precision.
            if (desiredClass == java.sql.Date.class) {
                actualClass = java.sql.Timestamp.class;
                suffix = "Timestamp";
            } else {
                return null;
            }
            break;

        case TIME:
            if (desiredClass == java.sql.Time.class) {
                actualClass = java.sql.Time.class;
                suffix = "Time";
            } else {
                return null;
            }
            break;

        case TIMESTAMP:
            if (desiredClass == java.sql.Timestamp.class) {
                actualClass = java.sql.Timestamp.class;
                suffix = "Timestamp";
            } else {
                return null;
            }
            break;

        case BINARY:
        case VARBINARY:
        case LONGVARBINARY:
            if (desiredClass == byte[].class) {
                actualClass = byte[].class;
                suffix = "Bytes";
            } else {
                return null;
            }
            break;

        case BLOB:
            if (desiredClass == com.amazon.carbonado.lob.Blob.class) {
                actualClass = java.sql.Blob.class;
                suffix = "Blob";
            } else {
                return null;
            }
            break;

        case CLOB:
            if (desiredClass == com.amazon.carbonado.lob.Clob.class) {
                actualClass = java.sql.Clob.class;
                suffix = "Clob";
            } else {
                return null;
            }
            break;
        }

        return new AccessInfo(suffix, actualClass);
    }

    /**
     * Appends words to a sentence as an "or" list.
     */
    private static void appendToSentence(StringBuilder buf, String[] names) {
        for (int i=0; i<names.length; i++) {
            if (i > 0) {
                if (i + 1 >= names.length) {
                    buf.append(" or ");
                } else {
                    buf.append(", ");
                }
            }
            buf.append('"');
            buf.append(names[i]);
            buf.append('"');
        }
    }

    /**
     * Generates aliases for the given name, converting camel case form into
     * various underscore forms.
     */
    static String[] generateAliases(String base) {
        int length = base.length();
        if (length <= 1) {
            return new String[]{base.toUpperCase(), base.toLowerCase()};
        }

        ArrayList<String> aliases = new ArrayList<String>(4);

        StringBuilder buf = new StringBuilder();

        int i;
        for (i=0; i<length; ) {
            char c = base.charAt(i++);
            if (c == '_' || !Character.isJavaIdentifierPart(c)) {
                // Keep scanning for first letter.
                buf.append(c);
            } else {
                buf.append(Character.toUpperCase(c));
                break;
            }
        }

        boolean canSeparate = false;
        boolean appendedIdentifierPart = false;

        for (; i<length; i++) {
            char c = base.charAt(i);
            if (c == '_' || !Character.isJavaIdentifierPart(c)) {
                canSeparate = false;
                appendedIdentifierPart = false;
            } else if (Character.isLowerCase(c)) {
                canSeparate = true;
                appendedIdentifierPart = true;
            } else {
                if (appendedIdentifierPart &&
                    i + 1 < length && Character.isLowerCase(base.charAt(i + 1))) {
                    canSeparate = true;
                }
                if (canSeparate) {
                    buf.append('_');
                }
                canSeparate = false;
                appendedIdentifierPart = true;
            }
            buf.append(c);
        }

        String derived = buf.toString();

        addToSet(aliases, derived.toUpperCase());
        addToSet(aliases, derived.toLowerCase());
        addToSet(aliases, derived);
        addToSet(aliases, base.toUpperCase());
        addToSet(aliases, base.toLowerCase());
        addToSet(aliases, base);

        return aliases.toArray(new String[aliases.size()]);
    }

    private static void addToSet(ArrayList<String> list, String value) {
        if (!list.contains(value)) {
            list.add(value);
        }
    }

    static String intern(String str) {
        return str == null ? null : str.intern();
    }

    private static class ColumnInfo {
        final String columnName;
        final int dataType;
        final String dataTypeName;
        final int columnSize;
        final int decimalDigits;
        final boolean nullable;
        final int charOctetLength;
        final int ordinalPosition;

        ColumnInfo(ResultSet rs) throws SQLException {
            columnName = intern(rs.getString("COLUMN_NAME"));
            dataTypeName = intern(rs.getString("TYPE_NAME"));
            columnSize = rs.getInt("COLUMN_SIZE");
            decimalDigits = rs.getInt("DECIMAL_DIGITS");
            nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
            charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
            ordinalPosition = rs.getInt("ORDINAL_POSITION");

            int dt = rs.getInt("DATA_TYPE");
            if (dt == OTHER) {
                if ("BLOB".equalsIgnoreCase(dataTypeName)) {
                    dt = BLOB;
                } else if ("CLOB".equalsIgnoreCase(dataTypeName)) {
                    dt = CLOB;
                } else if ("FLOAT".equalsIgnoreCase(dataTypeName)) {
                    dt = FLOAT;
                } else if ("TIMESTAMP".equalsIgnoreCase(dataTypeName)) {
                    dt = TIMESTAMP;
                } else if (dataTypeName.toUpperCase().contains("TIMESTAMP")) {
                    dt = TIMESTAMP;
                }
            } else if (dt == LONGVARBINARY && "BLOB".equalsIgnoreCase(dataTypeName)) {
                // Workaround MySQL bug.
                dt = BLOB;
            } else if (dt == LONGVARCHAR && "CLOB".equalsIgnoreCase(dataTypeName)) {
                // Workaround MySQL bug.
                dt = CLOB;
            }

            dataType = dt;
        }
    }

    private static class AccessInfo {
        // ResultSet get method, never null.
        final Method mResultSetGet;

        // PreparedStatement set method, never null.
        final Method mPreparedStatementSet;

        // Is null if no adapter needed.
        private StorablePropertyAdapter mAdapter;

        AccessInfo(String suffix, Class actualClass) {
            try {
                mResultSetGet = ResultSet.class.getMethod("get" + suffix, int.class);
                mPreparedStatementSet = PreparedStatement.class.getMethod
                    ("set" + suffix, int.class, actualClass);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }
        }

        StorablePropertyAdapter getAdapter() {
            return mAdapter;
        }

        void setAdapter(StorablePropertyAdapter adapter) {
            mAdapter = adapter;
        }
    }

    /**
     * Implementation of JDBCStorableInfo. The 'J' prefix is just a shorthand
     * to disambiguate the class name.
     */
    private static class JInfo<S extends Storable> implements JDBCStorableInfo<S> {
        private final StorableInfo<S> mMainInfo;
        private final String mCatalogName;
        private final String mSchemaName;
        private final String mTableName;
        private final String mQualifiedTableName;
        private final IndexInfo[] mIndexInfo;
        private final Map<String, JDBCStorableProperty<S>> mAllProperties;

        private transient Map<String, JDBCStorableProperty<S>> mPrimaryKeyProperties;
        private transient Map<String, JDBCStorableProperty<S>> mDataProperties;
        private transient Map<String, JDBCStorableProperty<S>> mIdentityProperties;
        private transient JDBCStorableProperty<S> mVersionProperty;

        JInfo(StorableInfo<S> mainInfo,
              String catalogName, String schemaName, String tableName, String qualifiedTableName,
              IndexInfo[] indexInfo,
              Map<String, JDBCStorableProperty<S>> allProperties)
        {
            mMainInfo = mainInfo;
            mCatalogName = intern(catalogName);
            mSchemaName = intern(schemaName);
            mTableName = intern(tableName);
            mQualifiedTableName = intern(qualifiedTableName);
            mIndexInfo = indexInfo;
            mAllProperties = Collections.unmodifiableMap(allProperties);
        }

        public String getName() {
            return mMainInfo.getName();
        }

        public Class<S> getStorableType() {
            return mMainInfo.getStorableType();
        }

        public StorableKey<S> getPrimaryKey() {
            return mMainInfo.getPrimaryKey();
        }

        public int getAlternateKeyCount() {
            return mMainInfo.getAlternateKeyCount();
        }

        public StorableKey<S> getAlternateKey(int index) {
            return mMainInfo.getAlternateKey(index);
        }

        public StorableKey<S>[] getAlternateKeys() {
            return mMainInfo.getAlternateKeys();
        }

        public int getAliasCount() {
            return mMainInfo.getAliasCount();
        }

        public String getAlias(int index) {
            return mMainInfo.getAlias(index);
        }

        public String[] getAliases() {
            return mMainInfo.getAliases();
        }

        public int getIndexCount() {
            return mMainInfo.getIndexCount();
        }

        public StorableIndex<S> getIndex(int index) {
            return mMainInfo.getIndex(index);
        }

        public StorableIndex<S>[] getIndexes() {
            return mMainInfo.getIndexes();
        }

        public boolean isIndependent() {
            return mMainInfo.isIndependent();
        }

        public boolean isAuthoritative() {
            return mMainInfo.isAuthoritative();
        }

        public boolean isSupported() {
            return mTableName != null;
        }

        public String getCatalogName() {
            return mCatalogName;
        }

        public String getSchemaName() {
            return mSchemaName;
        }

        public String getTableName() {
            return mTableName;
        }

        public String getQualifiedTableName() {
            return mQualifiedTableName;
        }

        public IndexInfo[] getIndexInfo() {
            return mIndexInfo.clone();
        }

        public Map<String, JDBCStorableProperty<S>> getAllProperties() {
            return mAllProperties;
        }

        public Map<String, JDBCStorableProperty<S>> getPrimaryKeyProperties() {
            if (mPrimaryKeyProperties == null) {
                Map<String, JDBCStorableProperty<S>> pkProps =
                    new LinkedHashMap<String, JDBCStorableProperty<S>>(mAllProperties.size());
                for (Map.Entry<String, JDBCStorableProperty<S>> entry : mAllProperties.entrySet()){
                    JDBCStorableProperty<S> property = entry.getValue();
                    if (property.isPrimaryKeyMember()) {
                        pkProps.put(entry.getKey(), property);
                    }
                }
                mPrimaryKeyProperties = Collections.unmodifiableMap(pkProps);
            }
            return mPrimaryKeyProperties;
        }

        public Map<String, JDBCStorableProperty<S>> getDataProperties() {
            if (mDataProperties == null) {
                Map<String, JDBCStorableProperty<S>> dataProps =
                    new LinkedHashMap<String, JDBCStorableProperty<S>>(mAllProperties.size());
                for (Map.Entry<String, JDBCStorableProperty<S>> entry : mAllProperties.entrySet()){
                    JDBCStorableProperty<S> property = entry.getValue();
                    if (!property.isPrimaryKeyMember() && !property.isJoin()) {
                        dataProps.put(entry.getKey(), property);
                    }
                }
                mDataProperties = Collections.unmodifiableMap(dataProps);
            }
            return mDataProperties;
        }

        public Map<String, JDBCStorableProperty<S>> getIdentityProperties() {
            if (mIdentityProperties == null) {
                Map<String, JDBCStorableProperty<S>> idProps =
                    new LinkedHashMap<String, JDBCStorableProperty<S>>(1);
                for (Map.Entry<String, JDBCStorableProperty<S>> entry :
                         getPrimaryKeyProperties().entrySet())
                {
                    JDBCStorableProperty<S> property = entry.getValue();
                    if (property.isAutoIncrement()) {
                        idProps.put(entry.getKey(), property);
                    }
                }
                mIdentityProperties = Collections.unmodifiableMap(idProps);
            }
            return mIdentityProperties;
        }

        public JDBCStorableProperty<S> getVersionProperty() {
            if (mVersionProperty == null) {
                for (JDBCStorableProperty<S> property : mAllProperties.values()) {
                    if (property.isVersion()) {
                        mVersionProperty = property;
                        break;
                    }
                }
            }
            return mVersionProperty;
        }
    }

    /**
     * Implementation of JDBCStorableProperty. The 'J' prefix is just a
     * shorthand to disambiguate the class name.
     */
    private static class JProperty<S extends Storable> implements JDBCStorableProperty<S> {
        private static final long serialVersionUID = -7333912817502875485L;

        private final StorableProperty<S> mMainProperty;
        private final String mColumnName;
        private final Integer mDataType;
        private final String mDataTypeName;
        private final boolean mColumnNullable;
        private final Method mResultSetGet;
        private final Method mPreparedStatementSet;
        private final StorablePropertyAdapter mAdapter;
        private final Integer mColumnSize;
        private final Integer mDecimalDigits;
        private final Integer mCharOctetLength;
        private final Integer mOrdinalPosition;
        private final boolean mAutoIncrement;
        private final boolean mPrimaryKeyCheckDisabled;

        private JDBCStorableProperty<S>[] mInternal;
        private JDBCStorableProperty<?>[] mExternal;

        /**
         * Join properties need to be filled in later.
         */
        JProperty(StorableProperty<S> mainProperty,
                  ColumnInfo columnInfo,
                  boolean autoIncrement,
                  boolean primaryKeyCheckDisabled,
                  Method resultSetGet,
                  Method preparedStatementSet,
                  StorablePropertyAdapter adapter)
        {
            mMainProperty = mainProperty;
            mColumnName = columnInfo.columnName;
            mDataType = columnInfo.dataType;
            mDataTypeName = columnInfo.dataTypeName;
            mColumnNullable = columnInfo.nullable;
            mResultSetGet = resultSetGet;
            mPreparedStatementSet = preparedStatementSet;
            mAdapter = adapter;
            mColumnSize = columnInfo.columnSize;
            mDecimalDigits = columnInfo.decimalDigits;
            mCharOctetLength = columnInfo.charOctetLength;
            mOrdinalPosition = columnInfo.ordinalPosition;
            mAutoIncrement = autoIncrement;
            mPrimaryKeyCheckDisabled = primaryKeyCheckDisabled;
        }

        JProperty(StorableProperty<S> mainProperty, boolean primaryKeyCheckDisabled) {
            mMainProperty = mainProperty;
            mColumnName = null;
            mDataType = null;
            mDataTypeName = null;
            mColumnNullable = false;
            mResultSetGet = null;
            mPreparedStatementSet = null;
            mAdapter = null;
            mColumnSize = null;
            mDecimalDigits = null;
            mCharOctetLength = null;
            mOrdinalPosition = null;
            mAutoIncrement = false;
            mPrimaryKeyCheckDisabled = primaryKeyCheckDisabled;
        }

        public String getName() {
            return mMainProperty.getName();
        }

        public String getBeanName() {
            return mMainProperty.getBeanName();
        }

        public Class<?> getType() {
            return mMainProperty.getType();
        }

        public Class<?>[] getCovariantTypes() {
            return mMainProperty.getCovariantTypes();
        }

        public int getNumber() {
            return mMainProperty.getNumber();
        }

        public Class<S> getEnclosingType() {
            return mMainProperty.getEnclosingType();
        }

        public Method getReadMethod() {
            return mMainProperty.getReadMethod();
        }

        public String getReadMethodName() {
            return mMainProperty.getReadMethodName();
        }

        public Method getWriteMethod() {
            return mMainProperty.getWriteMethod();
        }

        public String getWriteMethodName() {
            return mMainProperty.getWriteMethodName();
        }

        public boolean isNullable() {
            return mMainProperty.isNullable();
        }

        public boolean isPrimaryKeyMember() {
            return mMainProperty.isPrimaryKeyMember();
        }

        public boolean isAlternateKeyMember() {
            return mMainProperty.isAlternateKeyMember();
        }

        public int getAliasCount() {
            return mMainProperty.getAliasCount();
        }

        public String getAlias(int index) {
            return mMainProperty.getAlias(index);
        }

        public String[] getAliases() {
            return mMainProperty.getAliases();
        }

        public boolean isJoin() {
            return mMainProperty.isJoin();
        }

        public boolean isOneToOneJoin() {
            return mMainProperty.isOneToOneJoin();
        }

        public Class<? extends Storable> getJoinedType() {
            return mMainProperty.getJoinedType();
        }

        public int getJoinElementCount() {
            return mMainProperty.getJoinElementCount();
        }

        public boolean isQuery() {
            return mMainProperty.isQuery();
        }

        public int getConstraintCount() {
            return mMainProperty.getConstraintCount();
        }

        public StorablePropertyConstraint getConstraint(int index) {
            return mMainProperty.getConstraint(index);
        }

        public StorablePropertyConstraint[] getConstraints() {
            return mMainProperty.getConstraints();
        }

        public StorablePropertyAdapter getAdapter() {
            return mMainProperty.getAdapter();
        }

        public String getSequenceName() {
            return mMainProperty.getSequenceName();
        }

        public boolean isAutomatic() {
            return mMainProperty.isAutomatic();
        }

        public boolean isVersion() {
            return mMainProperty.isVersion();
        }

        public boolean isIndependent() {
            return mMainProperty.isIndependent();
        }

        public boolean isDerived() {
            return mMainProperty.isDerived();
        }

        public ChainedProperty<S>[] getDerivedFromProperties() {
            return mMainProperty.getDerivedFromProperties();
        }

        public ChainedProperty<?>[] getDerivedToProperties() {
            return mMainProperty.getDerivedToProperties();
        }

        public boolean shouldCopyDerived() {
            return mMainProperty.shouldCopyDerived();
        }

        public boolean isSupported() {
            if (isJoin()) {
                // TODO: Check if joined type is supported
                return true;
            } else {
                return mColumnName != null;
            }
        }

        public boolean isSelectable() {
            return mColumnName != null && !isJoin() && !isDerived();
        }

        public boolean isAutoIncrement() {
            return mAutoIncrement;
        }

        public String getColumnName() {
            return mColumnName;
        }

        public Integer getDataType() {
            return mDataType;
        }

        public String getDataTypeName() {
            return mDataTypeName;
        }

        public boolean isColumnNullable() {
            return mColumnNullable;
        }

        public Method getResultSetGetMethod() {
            return mResultSetGet;
        }

        public Method getPreparedStatementSetMethod() {
            return mPreparedStatementSet;
        }

        public StorablePropertyAdapter getAppliedAdapter() {
            return mAdapter;
        }

        public Integer getColumnSize() {
            return mColumnSize;
        }

        public Integer getDecimalDigits() {
            return mDecimalDigits;
        }

        public Integer getCharOctetLength() {
            return mCharOctetLength;
        }

        public Integer getOrdinalPosition() {
            return mOrdinalPosition;
        }

        public JDBCStorableProperty<S> getInternalJoinElement(int index) {
            if (mInternal == null) {
                throw new IndexOutOfBoundsException();
            }
            return mInternal[index];
        }

        @SuppressWarnings("unchecked")
        public JDBCStorableProperty<S>[] getInternalJoinElements() {
            if (mInternal == null) {
                return new JDBCStorableProperty[0];
            }
            return mInternal.clone();
        }

        public JDBCStorableProperty<?> getExternalJoinElement(int index) {
            if (mExternal == null) {
                throw new IndexOutOfBoundsException();
            }
            return mExternal[index];
        }

        public JDBCStorableProperty<?>[] getExternalJoinElements() {
            if (mExternal == null) {
                return new JDBCStorableProperty[0];
            }
            return mExternal.clone();
        }

        @Override
        public String toString() {
            return mMainProperty.toString();
        }

        public void appendTo(Appendable app) throws IOException {
            mMainProperty.appendTo(app);
        }

        @SuppressWarnings("unchecked")
        void fillInternalJoinElements(DataSource ds, String catalog, String schema,
                                      SchemaResolver resolver)
            throws SQLException, SupportException
        {
            StorableProperty<S>[] mainInternal = mMainProperty.getInternalJoinElements();
            if (mainInternal.length == 0) {
                mInternal = null;
                return;
            }

            JDBCStorableInfo<S> info = examine(getEnclosingType(), ds, catalog, schema, resolver, mPrimaryKeyCheckDisabled);

            JDBCStorableProperty<S>[] internal = new JDBCStorableProperty[mainInternal.length];
            for (int i=mainInternal.length; --i>=0; ) {
                internal[i] = info.getAllProperties().get(mainInternal[i].getName());
            }
            mInternal = internal;
        }

        void fillExternalJoinElements(DataSource ds, String catalog, String schema,
                                      SchemaResolver resolver)
            throws SQLException, SupportException
        {
            StorableProperty<?>[] mainExternal = mMainProperty.getExternalJoinElements();
            if (mainExternal.length == 0) {
                mExternal = null;
                return;
            }

            JDBCStorableInfo<?> info = examine(getJoinedType(), ds, catalog, schema, resolver, mPrimaryKeyCheckDisabled);

            JDBCStorableProperty<?>[] external = new JDBCStorableProperty[mainExternal.length];
            for (int i=mainExternal.length; --i>=0; ) {
                external[i] = info.getAllProperties().get(mainExternal[i].getName());
            }
            mExternal = external;
        }
    }
}
