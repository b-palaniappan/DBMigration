package io.c12.bala.datacopy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * 
 * @author b.palaniappan
 * From  - https://github.com/vijayan007/data-copier
 * Copy tables - https://stackoverflow.com/questions/11268057/how-to-create-table-based-on-jdbc-result-set
 */
public class DataCopier {

	private static final Logger LOGGER = Logger.getLogger(DataCopier.class.getName());
	private Database source;
	private Database destination;
	private List<String> sqls;

	public static void main(String[] args) {

		DataCopier copier = getInstance();
		LOGGER.info(copier.toString());

		Connection srcCon = copier.source.openConnection();
		Connection destCon = copier.destination.openConnection();

		try {
			for (String sql : copier.sqls) {
				try {
					Statement srcStmt = srcCon.createStatement();
					LOGGER.info("Source Select SQL:" + sql);
					ResultSet srcRs = srcStmt.executeQuery(sql);
					ResultSetMetaData srcRsMd = srcRs.getMetaData();

					LOGGER.info("Running setAutoCommit(false) mode");
					destCon.setAutoCommit(false);
					Statement destStmt = destCon.createStatement();
					
					String tableName = getTableName(sql);
					String destSelectSql = "SELECT * FROM " + tableName + " WHERE 1=2";
					LOGGER.info("Destination Select SQL:" + destSelectSql);

					ResultSet destRs = destStmt.executeQuery(destSelectSql);
					ResultSetMetaData destRsMd = destRs.getMetaData();

					if (destRsMd.getColumnCount() < srcRsMd.getColumnCount()) {
						throw new IllegalArgumentException(sql + " - number of coulmns do not match");
					}

					HashMap<String, Integer> destColumnNames = getColumnNames(destRsMd);

					List<String> columnNames = new ArrayList<String>();
					List<String> columnParams = new ArrayList<String>();

					for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
						String srcCoulmnName = srcRsMd.getColumnName(i);
						if (destColumnNames.get(srcCoulmnName) == null) {
							throw new IllegalArgumentException(
									"could not find " + srcCoulmnName + " in destination db");
						}
						columnNames.add(srcCoulmnName);
						columnParams.add("?");
					}

					String columns = columnNames.toString().replace('[', '(').replace(']', ')');
					String params = columnParams.toString().replace('[', '(').replace(']', ')');

					String destInsertSql = "INSERT INTO " + tableName + " " + columns + " VALUES " + params;
					LOGGER.info("Destination Insert SQL:" + destInsertSql);
					PreparedStatement destInsertStmt = destCon.prepareStatement(destInsertSql);

					while (srcRs.next()) {
						for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
							destInsertStmt.setObject(i, srcRs.getObject(i));
						}
						try {
							destInsertStmt.executeUpdate();
						} catch (SQLException e) {
							// Assuming the PK is first column
							LOGGER.warning("Skipping row where " + srcRsMd.getColumnName(1) + " = " + srcRs.getObject(1)
									+ " because of " + e.getMessage());
						}
					}
					srcRs.close();
					srcStmt.close();
					destRs.close();
					destStmt.close();
					destInsertStmt.close();
				} catch (SQLException e) {
					LOGGER.warning("Skipping executing sql = " + sql + " because of " + e.getMessage());
				}
			}
			destCon.commit();
			LOGGER.info("Destination DB is committed successfully");
		} catch (SQLException e) {
			rollback(destCon);
			LOGGER.severe("Major issues becuase of " + e.getMessage());
		} finally {
			LOGGER.info("Closing connections");
			Database.closeConnection(destCon);
			Database.closeConnection(srcCon);
			LOGGER.info("Closed connections successfully");
		}
	}

	private static String getTableName(String sql) {

		String tableName = null;

		int startIndex = sql.indexOf("FROM ");

		// try for small case
		if (startIndex == -1) {
			startIndex = sql.indexOf("from ");
		}

		if (startIndex == -1) {
			return tableName;
		} else {
			startIndex = startIndex + 5;
		}

		int endIndex = sql.indexOf(" WHERE");
		// try for small case
		if (endIndex == -1) {
			endIndex = sql.indexOf(" where");
		}

		if (endIndex != -1) {
			tableName = sql.substring(startIndex, endIndex);
		} else {
			tableName = sql.substring(startIndex);
		}

		return tableName.trim();
	}

	private static void rollback(Connection con) {
		try {
			con.rollback();
		} catch (SQLException ex) {
			// Do Nothing
		}
	}

	private static HashMap<String, Integer> getColumnNames(ResultSetMetaData metaData) throws SQLException {
		HashMap<String, Integer> columnNames = new HashMap<String, Integer>();
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			columnNames.put(metaData.getColumnName(i), i);
		}
		return columnNames;
	}

	public DataCopier() {
		this.source = new Database();
		this.destination = new Database();
		this.sqls = new ArrayList<String>();
	}

	public void addSql(String sql) {
		this.sqls.add(sql);
	}

	public static DataCopier getInstance(String fileName) {
		if (fileName != null) {
			throw new IllegalArgumentException("please provide input-file");
		}
		
		try {
			FileInputStream in = new FileInputStream(fileName);

			DataCopier copier = new DataCopier();

			Scanner scanner = new Scanner(in);
			copier.source.setJdbcDriverClassName(scanner.nextLine());
			copier.source.setJdbcUrl(scanner.nextLine());
			copier.source.setUsername(scanner.nextLine());
			copier.source.setPassword(scanner.nextLine());

			copier.destination.setJdbcDriverClassName(scanner.nextLine());
			copier.destination.setJdbcUrl(scanner.nextLine());
			copier.destination.setUsername(scanner.nextLine());
			copier.destination.setPassword(scanner.nextLine());

			while (scanner.hasNextLine()) {
				copier.addSql(scanner.nextLine());
			}

			scanner.close();
			in.close();

			return copier;

		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("unable to locate input-file = " + fileName);
		} catch (IOException e) {
			throw new RuntimeException("unable to close the input stream");
		}
	}
	
	public static final DataCopier getInstance() {
		DataCopier copier = new DataCopier();
		ResourceBundle resource  = ResourceBundle.getBundle("application");
		
		copier.source.setJdbcDriverClassName(resource.getString("jdbc.source.driver.classname"));
		copier.source.setJdbcUrl(resource.getString("jdbc.source.url"));
		copier.source.setUsername(resource.getString("jdbc.source.userid"));
		copier.source.setPassword(resource.getString("jdbc.source.password"));

		copier.destination.setJdbcDriverClassName(resource.getString("jdbc.target.driver.classname"));
		copier.destination.setJdbcUrl(resource.getString("jdbc.target.url"));
		copier.destination.setUsername(resource.getString("jdbc.target.userid"));
		copier.destination.setPassword(resource.getString("jdbc.target.password"));
		
		copier.addSql("SELECT * FROM USER");
		
		return copier;
	}

	public String toString() {
		return "DataCopier {source=" + source + ", destination=" + destination + ", sqls=" + sqls + "}";
	}
}
