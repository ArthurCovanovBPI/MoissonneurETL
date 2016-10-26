package rfharvester.upload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;

public class UploadCollectionsMySQL5V2 implements RFHarvesterUploaderV2Interface
{
	private final String uploadDB;
	private final String uploadDBlogin = "harvester";
	private final String uploadDBpassword = "harvester";
	private final String tableName = "collections";
	private Connection uploadDBConnection = null;

	private final int collection_id;
	private int storedRows = 0;

	private void initTable() throws SQLException
	{
		String query;
		Statement uploadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		uploadDBStatement = uploadDBConnection.createStatement(type, mode);

		query = "DROP TABLE IF EXISTS " + tableName + "_new";
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);
		
		//Create the filled table
		query = "CREATE TABLE " + tableName + "_new" + " LIKE " + tableName;
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);
		
		//Fill table with old datas
		query = "INSERT INTO " + tableName + "_new" + " (SELECT * FROM " + tableName + ")";
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);
		try
		{
			if(uploadDBStatement != null)
				uploadDBStatement.close();
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to close external database statement:\n                                                 " + RFHarvesterLogger.exceptionToString(e));
		}
	}

	public UploadCollectionsMySQL5V2(String uploadDB, int collectionId) throws SQLException, ClassNotFoundException
	{
		this.uploadDB = "jdbc:mysql://" + uploadDB + "?characterEncoding=ISO-8859-1&autoReconnect=true";

		collection_id = collectionId;
		RFHarvesterLogger.info("Initializing " + this.getClass().getName());
		String jdbc = "Fatal error NOJDBC";

		jdbc = "com.mysql.jdbc.Driver";
		RFHarvesterLogger.debug("Init " + jdbc);
		Class.forName(jdbc);
		uploadDBConnection = DriverManager.getConnection(this.uploadDB, uploadDBlogin, uploadDBpassword);
		RFHarvesterLogger.info("Connection with " + this.uploadDB + " established");
		initTable();
	}

	@Override
	public String insertRow(HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2Exception
	{
		storedRows++;
		return ""+storedRows;
	}

	@Override
	public void dropLast()
	{
		storedRows--;
	}

	@Override
	public void end() throws RFHarvesterUploaderV2Exception
	{
		try
		{
			replaceStoredValue();
			replaceOldTable();
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderV2Exception(e);
		}
	}

	private void replaceStoredValue() throws SQLException
	{
		String query;
		Statement uploadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		uploadDBStatement = uploadDBConnection.createStatement(type, mode);

		query = "UPDATE " + tableName + "_new SET nb_result = " + storedRows + " WHERE id = " + collection_id;
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);

		try
		{
			if(uploadDBStatement != null)
				uploadDBStatement.close();
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private void replaceOldTable() throws SQLException
	{
		String query;
		Statement uploadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		uploadDBStatement = uploadDBConnection.createStatement(type, mode);

		query = "DROP TABLE IF EXISTS " + tableName + "_old";
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);

		query = "ALTER TABLE " + tableName + " RENAME TO " + tableName + "_old";
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);

		query = "ALTER TABLE " + tableName + "_new" + " RENAME TO " + tableName;
		RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);

		try
		{
			if(uploadDBStatement != null)
				uploadDBStatement.close();
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	protected void finalize() throws Throwable
	{

		if(uploadDBConnection != null)
		{
			uploadDBConnection.close();
		}
	}
}
