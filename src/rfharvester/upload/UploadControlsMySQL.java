package rfharvester.upload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import rfharvester.logger.RFHarvesterLogger;

public class UploadControlsMySQL implements RFHarvesterUploaderInterface
{
//	private final String uploadDB = "jdbc:mysql://10.1.2.140/lf_prod?characterEncoding=ISO-8859-1";
	private final String uploadDB = "jdbc:mysql://10.1.2.113/lf_prod?characterEncoding=ISO-8859-1&autoReconnect=true";
	private final String uploadDBlogin = "root";
	private final String uploadDBpassword = "mysqlbpi";
	private final String tableName = "controls";
	private ArrayList<String> tableColumns = null;
	private String tableColumnsText = "";
	private Connection uploadDBConnection = null;

	private final int collection_id;
	private int storedRows = 0;
	private int recomandedCommitValue = 20000;
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

//			query = "DELETE FROM " + tableName + "_new WHERE collection_id = " + collection_id;
//			RFHarvesterLogger.debug(query);
//			uploadDBStatement.execute(query);
			
			//Fill table with old datas
			query = "INSERT INTO " + tableName + "_new" + " (SELECT * FROM " + tableName + " WHERE collection_id != " + collection_id + ")";
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
			
//			//Delete old datas
//			query = "DELETE FROM " + tableName + "_new" + " WHERE collection_id = " + collection_id;
//			RFHarvesterLogger.debug(query);
//			uploadDBStatement.execute(query);

			tableColumns = new ArrayList<String>();
			ResultSet dcnColumns = uploadDBConnection.getMetaData().getColumns("lf_prod", null, tableName, null);
			while(dcnColumns.next())
			{
				String curColumn = dcnColumns.getString(4);
				if(curColumn.compareTo("id")!=0)
					tableColumns.add(curColumn);
			}
			dcnColumns.close();
			tableColumnsText = tableColumns.toString();
			tableColumnsText = tableColumnsText.substring(1, tableColumnsText.length()-1);
			RFHarvesterLogger.debug(tableName + " columns : " + tableColumnsText);
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

	public UploadControlsMySQL(int collectionId)
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
//			initTables();
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
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "START TRANSACTION";
			//RFHarvesterLogger.debug(query);
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

	@Override
	public void insertRow(final HashMap<String, String> rows) throws RFHarvesterUploaderException
	{
		String query = "";
		Statement uploadDBStatement = null;
		String values = "";
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);
			for(String S : tableColumns)
			{
				//System.out.println("#" + S + " ~ " + rows.get(S));
				if(!rows.containsKey(S))
				{
					String keySet = "";
					String missingSet = "";
					Set<String> keys = rows.keySet();
					for(String K : keys)
						keySet += ("('"+K+"', '"+rows.get(K)+"')");
					for(String C : tableColumns)
						if(!rows.containsKey(C))
							missingSet += ("'" + C + "', ");
					missingSet = missingSet.substring(0, missingSet.length()-2);
					throw new RFHarvesterUploaderException("Error set:    "+ keySet + "]\n                                        Missing keys: " + missingSet);// + "\n                                        " + query);
				}
				String column = rows.get(S);
				if(column == null)
					values += ("NULL, ");
				else
					values += ("'" + column.replaceAll("'", "''") + "', ");
			}
			values = values.substring(0, values.length()-2);

			query = "INSERT INTO " + tableName + "_new" + " (" + tableColumnsText + ") VALUES(" + new String(values.getBytes(), "ISO-8859-1") + ")";
//			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);

			storedRows++;
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderException("Unable to insert: [" + values + "]\n                                        " + e.getClass().getName() + ": " + e.getMessage());// + "\n                                        " + query);
		}
		catch(RFHarvesterUploaderException e)
		{
			throw new RFHarvesterUploaderException(e);
		}
		catch(Exception e)
		{
			String errorMess = "";
			for(String S : tableColumns)
			{
				try
				{
					errorMess +=("\n   #" + S + " ~ " + rows.get(S));
				}
				catch(Exception c)
				{
					errorMess +=("\n   #" + S + " ~ FAILURE");
				}
			}

			throw new RFHarvesterUploaderException("FATAL ERROR!!!" + errorMess);
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

	HashSet<String> errors = new HashSet<String>();
	
	public void storeErrorIdentifier(String dc_identifier)
	{
		errors.add(dc_identifier);
	}

	public void cleanErrors()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			for(String error : errors)
			{
				query = "DELETE FROM " + tableName + "_new WHERE collection_id = " + collection_id + " AND oai_identifier = " + error;
				RFHarvesterLogger.debug(query);
				uploadDBStatement.execute(query);
			}
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

	@Override
	public void commit()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "COMMIT";
			//RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
			storedRows = 0;
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

	@Override
	public void mergeOldTable()
	{
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
