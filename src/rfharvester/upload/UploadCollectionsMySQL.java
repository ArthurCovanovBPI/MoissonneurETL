package rfharvester.upload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;

public class UploadCollectionsMySQL implements RFHarvesterUploaderInterface
{
//	private final String uploadDB = "jdbc:mysql://10.1.2.140/lf_prod?characterEncoding=ISO-8859-1";
	private final String uploadDB = "jdbc:mysql://10.1.2.113/lf_prod?characterEncoding=ISO-8859-1&autoReconnect=true";
	private final String uploadDBlogin = "root";
	private final String uploadDBpassword = "mysqlbpi";
	private final String tableName = "collections";
	private Connection uploadDBConnection = null;

	private final int collection_id;
	private int storedRows = 0;
	private int recomandedCommitValue = -1;
	private final String className = this.getClass().getName();

	public int getStoredRows()
	{
		return storedRows;
	}

	public int getRecomandedCommit()
	{
		return recomandedCommitValue;
	}

	public String getClassName()
	{
		return className;
	}

	public void initTable()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
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
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}
		finally
		{
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
	}

	public void copyIntoOld()
	{
	}

	public UploadCollectionsMySQL(int collectionId)
	{
		collection_id = collectionId;
		RFHarvesterLogger.info("Initializing " + className);
		String jdbc = "Fatal error NOJDBC";
		String curDB = "Fatal error NODBNAME";
		try
		{
			jdbc = "com.mysql.jdbc.Driver";
			RFHarvesterLogger.debug("Init " + jdbc);
			Class.forName(jdbc);
			curDB = uploadDB;
			uploadDBConnection = DriverManager.getConnection(uploadDB, uploadDBlogin, uploadDBpassword);
			RFHarvesterLogger.info("Connection with " + uploadDB + " established");
		}
		catch(ClassNotFoundException e)
		{
			RFHarvesterLogger.error(jdbc + "not found " + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with missing library.
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to establish connection with" + curDB + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with a missing library.
		}
	}

	@Override
	public void begin()
	{
	}

	@Override
	public void insertRow(final HashMap<String, String> rows) throws RFHarvesterUploadClassException
	{
		storedRows++;
	}

	private int errorsCount = 0;
	
	public void storeErrorIdentifier(String dc_identifier)
	{
		errorsCount++;
	}

	public void cleanErrors()
	{
		storedRows -= errorsCount;
	}

	@Override
	public void commit()
	{
	}

	@Override
	public void mergeOldTable()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "UPDATE " + tableName + "_new SET nb_result = " + storedRows + " WHERE id = " + collection_id;
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
		}
		finally
		{
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
	}

	@Override
	public void replaceOldTable()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "DROP TABLE IF EXISTS " + tableName + "_old";
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);

			query = "ALTER TABLE " + tableName + " RENAME TO " + tableName + "_old";
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
		}
		try
		{
			query = "ALTER TABLE " + tableName + "_new" + " RENAME TO " + tableName;
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
		}
		finally
		{
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
