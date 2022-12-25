package io.c12.bala.datacopy;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;

/**
 * @author b.palaniappan
 * <p>
 * To drop all the tables in a schema -  DROP SCHEMA if exists public.public cascade;
 * <p>
 * This program copies the table structure from source system and then all the primary key and foreign keys.
 * Tables, which need to be copied are in tables.txt and domain table names are in domainTableList.txt
 */
public class CopyTableAndData {

    private static final String SPACE = " ";
    private static final String UNDERSCORE = "_";
    private static final String COMMAN = ",";
    // TODO: Make sure the values are correct for the DB its used
    private static final Integer[] SIZE_REQUIRED = {-4, -3, -2, -1, 1, 12, 2004, 2005};
    private static final Integer[] SIZE_DECIMAL_REQUIRED = {3, 8};
    private static final int BATCH_INSERT_SIZE = 1000;
    private static final boolean COPY_DATA = false;
    private static final boolean COPY_DOMAIN_TABLES_ONLY = true;

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        long startTime = System.currentTimeMillis();
        ResourceBundle resource = ResourceBundle.getBundle("application");
        List<String> tableNames = getTableName();
        String schemaName = resource.getString("jdbc.db2.schema");

        Connection destinationConnection = getHyperSqlConnection(resource);
        Connection sourceConnection = getDB2Connection(resource);

        // -- Step 1: Create table and add PK constraints
        for (String tableName : tableNames) {
            System.out.println("-- " + tableName);
            String createTableQuery = createTableQuery(sourceConnection, tableName);
            executeUpdate(destinationConnection, createTableQuery);
            String primaryKeyConstraintQuery = createPkConstraintQuery(sourceConnection, tableName, schemaName);
            executeUpdate(destinationConnection, primaryKeyConstraintQuery);
        }

        // -- Step 2: copy data using insert
        if (COPY_DATA) {
            System.out.println("-- Copy data");
            copyData(sourceConnection, destinationConnection, tableNames);
        }

        // -- Step 2.1: copy domain table data using insert
        if (COPY_DOMAIN_TABLES_ONLY) {
            System.out.println("-- Copy Domain data");
            List<String> domainTalbes = getDomainTableNames();
            copyData(sourceConnection, destinationConnection, domainTalbes);
        }

        System.out.println("-- Create FK constraints");
        // -- Step 3: Create FK constraints after tables are created.
        for (String tableName : tableNames) {
            List<String> foreignKeyQueries = createFkConstraintQuery(sourceConnection, tableName, schemaName, tableNames);
            for (String foreginKeyQuery : foreignKeyQueries) {
                System.out.println(foreginKeyQuery);
                executeUpdate(destinationConnection, foreginKeyQuery);
            }
        }

        System.out.println("-- Identity setup");
        // -- Step 4: Add autoIncrement after data moved
        for (String tableName : tableNames) {
            String addIdentiyQuery = createIdentityColumns(sourceConnection, tableName);
            if (addIdentiyQuery != null && !addIdentiyQuery.isEmpty()) {
                System.out.println(addIdentiyQuery);
                executeUpdate(destinationConnection, addIdentiyQuery);

                // -- Step 5: Update identity restart value
                String updateIdentityValueQuery = updateIdentityValue(destinationConnection, tableName);
                if (updateIdentityValueQuery != null && !updateIdentityValueQuery.isEmpty()) {
                    System.out.println(updateIdentityValueQuery);
                    executeUpdate(destinationConnection, updateIdentityValueQuery);
                }
            }
        }

        closeConnection(sourceConnection);
        closeConnection(destinationConnection);
        System.out.println("Time taken " + (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * Create query to create table in target DB
     *
     * @param connection - database connection
     * @param tableName  name of the table for create query.
     * @return Query to create table
     * @throws SQLException on any db exception.
     */
    private static String createTableQuery(Connection connection, String tableName) throws SQLException {
        String createQuery;
        try (PreparedStatement prepStmt = connection.prepareStatement("SELECT * FROM " + tableName + " where 1=2")) {
            ResultSet rs = prepStmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ").append(tableName).append(" ( ");
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                sb.append(rsMeta.getColumnName(i)).append(SPACE);
                // For varbinary used only in bank_acct_info
                if (rsMeta.getColumnType(i) == -3) {
                    sb.append("VARBINARY(").append(rsMeta.getPrecision(i)).append(")");
                } else if (Arrays.asList(SIZE_REQUIRED).contains(rsMeta.getColumnType(i))) {
                    sb.append(rsMeta.getColumnTypeName(i));
                    sb.append("(").append(rsMeta.getPrecision(i)).append(")");
                } else if (Arrays.asList(SIZE_DECIMAL_REQUIRED).contains(rsMeta.getColumnType(i))) {
                    sb.append(rsMeta.getColumnTypeName(i));
                    sb.append("(").append(rsMeta.getPrecision(i)).append(COMMAN).append(rsMeta.getScale(i)).append(")");
                } else {
                    sb.append(rsMeta.getColumnTypeName(i));
                }
                sb.append(rsMeta.isNullable(i) == 0 ? " NOT NULL, " : ", ");
            }
            createQuery = sb.substring(0, sb.toString().length() - 2) + " )";
            System.out.println(createQuery);
            rs.close();
        }
        return createQuery;
    }

    /**
     * Create a primary key constraints for the tables copied over to target DB
     *
     * @param connection - database connection
     * @param tableName  name of the table to create primary key.
     * @param schemaName of where the table is in DB
     * @return Query to add Primary key to copied over table
     * @throws SQLException on any db exception.
     */
    private static String createPkConstraintQuery(Connection connection, String tableName, String schemaName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT PK_").append(tableName).append(" PRIMARY KEY (");
        // -- Primary Key
        DatabaseMetaData dm = connection.getMetaData();
        ResultSet resultSet = dm.getPrimaryKeys(null, schemaName, tableName);
        while (resultSet.next()) {
            String pkey = resultSet.getString(4);
            sb.append(pkey).append(", ");
        }
        String primaryKeyConstraintQuery = sb.substring(0, sb.toString().length() - 2) + ")";
        System.out.println(primaryKeyConstraintQuery);
        resultSet.close();
        return primaryKeyConstraintQuery;
    }

    /**
     * Creates a Foreign key constraints for the tables copied over to target DB.
     *
     * @param connection database connection
     * @param tableName  name of the table to create FK constraint
     * @param schemaName of where the table is in DB
     * @param tableList  List of table to check for Foreign key
     * @return Query to add Foreign Key Constraint
     * @throws SQLException on any db exception.
     */
    private static List<String> createFkConstraintQuery(Connection connection, String tableName, String schemaName, List<String> tableList) throws SQLException {
        DatabaseMetaData dm = connection.getMetaData();
        ResultSet resultSet = dm.getImportedKeys(null, schemaName, tableName);
        List<String> foreignKeyQueries = new ArrayList<>();

        while (resultSet.next()) {
            if (tableList.contains(resultSet.getString(3))) {
                String sb = "ALTER TABLE " + tableName + " ADD CONSTRAINT FK_" +
                        resultSet.getString(3) + UNDERSCORE + resultSet.getString(7) +
                        " FOREIGN KEY (" +
                        resultSet.getString(8) + ") REFERENCES " +
                        resultSet.getString(3) + "(" + resultSet.getString(4) + ")";
                foreignKeyQueries.add(sb);
            }
        }
        resultSet.close();
        return foreignKeyQueries;
    }

    /**
     * Copy data from Source DB table to Destination DB table
     *
     * @param srcConn       Source DB connection
     * @param destConn      Destination DB connection.
     * @param tableNameList list of tables which data need to be copied.
     */
    private static void copyData(Connection srcConn, Connection destConn, List<String> tableNameList) {
        try {
            for (String tableName : tableNameList) {
                String sourceSelectQuery = "SELECT * FROM " + tableName;
                PreparedStatement srcPrepStmt = srcConn.prepareStatement(sourceSelectQuery);

                ResultSet srcRs = srcPrepStmt.executeQuery();
                ResultSetMetaData srcRsMd = srcPrepStmt.getMetaData();

                destConn.setAutoCommit(false);
                String destSelectQuery = "SELECT * FROM " + tableName + " WHERE 1=2";
                PreparedStatement destPrepStmt = destConn.prepareStatement(destSelectQuery);
                ResultSet destRs = destPrepStmt.executeQuery();
                ResultSetMetaData destRsMd = destPrepStmt.getMetaData();

                if (destRsMd.getColumnCount() != srcRsMd.getColumnCount()) {
                    throw new IllegalArgumentException(tableName + " - number of coulmns do not match");
                }

                HashMap<String, Integer> destColumnNames = getColumnNames(destRsMd);

                List<String> columnNames = new ArrayList<>();
                List<String> columnParams = new ArrayList<>();

                for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
                    String srcCoulmnName = srcRsMd.getColumnName(i);
                    if (destColumnNames.get(srcCoulmnName) == null) {
                        throw new IllegalArgumentException("could not find " + srcCoulmnName + " in destination table " + tableName);
                    }
                    columnNames.add(srcCoulmnName);
                    columnParams.add("?");
                }

                String columns = columnNames.toString().replace('[', '(').replace(']', ')');
                String params = columnParams.toString().replace('[', '(').replace(']', ')');

                String destInsertSql = "INSERT INTO " + tableName + " " + columns + " VALUES " + params;
                System.out.println(destInsertSql);

                PreparedStatement destInsertStmt = destConn.prepareStatement(destInsertSql);

                int count = 0;
                while (srcRs.next()) {
                    for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
                        destInsertStmt.setObject(i, srcRs.getObject(i));
                    }
                    destInsertStmt.addBatch();
                    try {
                        if (++count % BATCH_INSERT_SIZE == 0) {
                            destInsertStmt.executeBatch();
                        }
                    } catch (SQLException e) {
                        // Assuming the PK is first column
                        System.err.println("Skipping row where " + srcRsMd.getColumnName(1) + " = " + srcRs.getObject(1) + " because of " + e.getMessage());
                    }
                }
                if (count % BATCH_INSERT_SIZE != 0) {
                    destInsertStmt.executeBatch();
                }
                destInsertStmt.close();
                destRs.close();
                destPrepStmt.close();
                srcRs.close();
                srcPrepStmt.close();
            }
            destConn.commit();
        } catch (SQLException e1) {
            e1.printStackTrace();
            try {
                destConn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * If the Source Table have Identity, its creates an identity in target table
     *
     * @param db2Conn   Source database connection.
     * @param tableName to check if it has identity.
     * @return Query to create identity for PK
     * @throws SQLException on any sql failure.
     */
    private static String createIdentityColumns(Connection db2Conn, String tableName) throws SQLException {
        StringBuilder sb;
        try (PreparedStatement prepStmt = db2Conn.prepareStatement("SELECT * FROM " + tableName + " where 1=2")) {
            ResultSet rs = prepStmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            sb = new StringBuilder();
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                if (rsMeta.isAutoIncrement(i)) {
                    sb.append("ALTER TABLE ").append(tableName).append(" ALTER COLUMN ").append(rsMeta.getColumnName(i)).append(" identity");
                }
            }
            rs.close();
        }
        return sb.toString();
    }

    /**
     * Get the latest primary key value and update identity base value to Max of key +1
     *
     * @param hyperSqlConn destination connection.
     * @param tableName    table name to update identity.
     * @return Query to Update Identity value for newly created tables.
     * @throws SQLException on any sql failure.
     */
    private static String updateIdentityValue(Connection hyperSqlConn, String tableName) throws SQLException {
        StringBuilder sb;
        try (PreparedStatement prepStmt = hyperSqlConn.prepareStatement("SELECT * FROM " + tableName + " where 1=2")) {
            ResultSet rs = prepStmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            sb = new StringBuilder();

            // -- get column which is identity
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                if (rsMeta.isAutoIncrement(i)) {
                    String getMaxIdentityValQuery = "SELECT MAX(" + rsMeta.getColumnName(i) + ") FROM " + tableName;
                    System.out.println(getMaxIdentityValQuery);
                    int maxIdentityCount;
                    try (PreparedStatement idenPrepStmt = hyperSqlConn.prepareStatement(getMaxIdentityValQuery)) {
                        ResultSet idenRs = idenPrepStmt.executeQuery();
                        maxIdentityCount = 0;
                        while (idenRs.next()) {
                            maxIdentityCount = idenRs.getInt(1);
                        }
                        idenRs.close();
                    }
                    if (maxIdentityCount > 0) {
                        sb.append("ALTER TABLE ").append(tableName).append(" ALTER COLUMN ").append(rsMeta.getColumnName(i));
                        sb.append(" RESTART WITH ").append(maxIdentityCount + 1);
                    }
                }
            }
            rs.close();
        }
        return sb.toString();
    }

    /**
     * Get columns names from ResultSet Meta Data
     *
     * @param metaData result set metadata
     * @return Hashmap of column and number
     * @throws SQLException on any DB error.
     */
    private static HashMap<String, Integer> getColumnNames(ResultSetMetaData metaData) throws SQLException {
        HashMap<String, Integer> columnNames = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columnNames.put(metaData.getColumnName(i), i);
        }
        return columnNames;
    }

    /**
     * Get list of table names to be migrated from text file. One table per line.
     *
     * @return List of table names
     * @throws IOException on not able to find text file or error reading it.
     */
    private static List<String> getTableName() throws IOException {
        Path path = FileSystems.getDefault().getPath("src/main/resources", "tables.txt");
        return Files.readAllLines(path);
    }

    /**
     * Get all domain table names from properties file
     *
     * @return List of domain table names
     * @throws IOException on not able to find text file or error reading it.
     */
    private static List<String> getDomainTableNames() throws IOException {
        Path path = FileSystems.getDefault().getPath("src/main/resources", "domainTableList.txt");
        return Files.readAllLines(path);
    }

    /**
     * Execute update query
     *
     * @param connection - database connection
     * @param query      - SQL query to run.
     * @throws SQLException for any query exception.
     */
    private static void executeUpdate(Connection connection, String query) throws SQLException {
        try (PreparedStatement prepStmt = connection.prepareStatement(query)) {
            prepStmt.executeUpdate();
        }
    }

    /**
     * Create DB2 Connection using connection parameters from properties file
     *
     * @param resource with configuration of database information
     * @return DB2 Connection
     * @throws SQLException           for any DB connection errors.
     * @throws ClassNotFoundException for DB driver classname not found.
     */
    private static Connection getDB2Connection(ResourceBundle resource) throws SQLException, ClassNotFoundException {
        Class.forName(resource.getString("jdbc.source.driver.classname"));
        return DriverManager.getConnection(resource.getString("jdbc.source.url"), resource.getString("jdbc.source.userid"), resource.getString("jdbc.source.password"));
    }

    /**
     * Create HyperSQL in-memory DB Connection using connection parameters from properties file
     *
     * @param resource with configuration of database information.
     * @return HyperSQL in-memory database connection
     * @throws ClassNotFoundException for DB driver classname not found.
     * @throws SQLException           for any DB connection errors.
     */
    private static Connection getHyperSqlConnection(ResourceBundle resource) throws ClassNotFoundException, SQLException {
        Class.forName(resource.getString("jdbc.target.driver.classname"));
        return DriverManager.getConnection(resource.getString("jdbc.target.url"), resource.getString("jdbc.target.userid"), resource.getString("jdbc.target.password"));
    }

    /**
     * Close DB Connection.
     *
     * @param connection database connection
     * @throws SQLException on connection close failure.
     */
    private static void closeConnection(Connection connection) throws SQLException {
        connection.close();
    }

}
