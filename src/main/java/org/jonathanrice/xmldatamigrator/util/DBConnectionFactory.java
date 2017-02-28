package org.jonathanrice.xmldatamigrator.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

public class DBConnectionFactory {

	protected static String DB_CONNECTION;
	protected static String DB_DRIVER;
	protected static String DB_USER;
	protected static String DB_PASS;

	private DBConnectionFactory() {
		// This is a singleton
	}

	/**
	 * @param args
	 * @throws Exception
	 * @throws ParseErrorException
	 * @throws ResourceNotFoundException
	 */
	public static void main(String[] args) throws ResourceNotFoundException,
			ParseErrorException, Exception {
		//Test out the DBConnection with a simple query
		Connection conn = getDBConnection();
		ResultSet rset = null;
		Statement stmt = null;
		stmt = conn.createStatement();

		rset = stmt.executeQuery("select * from dual");

		String caseNbr = "";

		while (rset.next()) {
			caseNbr = rset.getString(1);
			System.out.print(caseNbr);
		}
	}

	protected static void getConnectionParameters() {
		DB_CONNECTION = PropertyUtil.getProperties().getProperty("jdbc_con_str");
		DB_DRIVER = PropertyUtil.getProperties().getProperty("jdbc_driver_class");
		DB_USER = PropertyUtil.getProperties().getProperty("db_user");
		DB_PASS = PropertyUtil.getProperties().getProperty("db_pass");
	}

	public static Connection getDBConnection() {
		if (DB_CONNECTION == null) {
			getConnectionParameters();
		}
		Connection dbConnection = null;
		/*try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
		}

		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		*/
		// Load the Oracle JDBC driver
		try {
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		
		// Get a connection to a database
			dbConnection = DriverManager.getConnection(DB_CONNECTION,DB_USER,DB_PASS);
			dbConnection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dbConnection;
	}
	
	@SuppressWarnings("unchecked")
	public static List<HashMap> resultSetToArrayList(ResultSet rs)
			throws SQLException {
		ArrayList list = new ArrayList(50);
		while (rs.next()) {
			HashMap row = resultSetRowToHashMap(rs);
			list.add(row);
		}
		return list;
	}
	
	@SuppressWarnings("unchecked")
	public static HashMap resultSetRowToHashMap(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();

		HashMap row = new HashMap(columns);
		for (int i = 1; i <= columns; ++i) {
			row.put(md.getColumnName(i), rs.getObject(i));
		}

		return row;
	}
}
