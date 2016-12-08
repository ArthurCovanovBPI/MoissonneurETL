package rfharvester.upload;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;

public class UploadControlsMySQL5V2 implements RFHarvesterUploaderV2Interface
{
	private final String uploadDB;
	private final String uploadDBlogin = "harvester";
	private final String uploadDBpassword = "harvester";
	private final String tableName = "controls";

	private Connection uploadDBConnection = null;

	private final int collectionID;
	private final String collectionName;
	private boolean startedTransaction = false;
	private int storedRows = 0;
	private final int recomandedCommit;

	private static String lastID = "";

	public void dropLast()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "DELETE FROM " + tableName + "_new WHERE id = " + lastID;
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);

			storedRows--;
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to delete last id " + lastID + " into " + tableName + "\n" +e.getClass().getName() + ": " + e.getMessage());
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
		query = "INSERT INTO " + tableName + "_new" + " (SELECT * FROM " + tableName + " WHERE collection_id != " + collectionID + ")";
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

	public UploadControlsMySQL5V2(String uploadDB, int recomandedCommit, int collectionID, String collectionName) throws ClassNotFoundException, SQLException
	{
		this.uploadDB = "jdbc:mysql://" + uploadDB + "?characterEncoding=ISO-8859-1&autoReconnect=true";

		this.collectionID = collectionID;
		this.collectionName = collectionName;
		this.recomandedCommit=recomandedCommit;
		RFHarvesterLogger.info("Initializing " + this.getClass().getName());
		String jdbc = "Fatal error NOJDBC";

		jdbc = "com.mysql.jdbc.Driver";
		RFHarvesterLogger.debug("Init " + jdbc);
		Class.forName(jdbc);
		uploadDBConnection = DriverManager.getConnection(this.uploadDB, uploadDBlogin, uploadDBpassword);
		RFHarvesterLogger.info("Connection with " + this.uploadDB + " established");
		initTable();
	}

	public void begin() throws SQLException
	{
		String query;
		Statement uploadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		uploadDBStatement = uploadDBConnection.createStatement(type, mode);

		query = "START TRANSACTION";
		//RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);
		startedTransaction = true;

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

	public void commit() throws SQLException
	{
		if(startedTransaction == false)
			return;
		String query;
		Statement uploadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		uploadDBStatement = uploadDBConnection.createStatement(type, mode);

		query = "COMMIT";
		//RFHarvesterLogger.debug(query);
		uploadDBStatement.execute(query);
		RFHarvesterLogger.debug(storedRows + " stored rows in " + tableName);
		storedRows = 0;
		startedTransaction = false;

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
	public String insertRow(HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2Exception
	{
		if(startedTransaction == false)
		{
			try
			{
				begin();
			}
			catch(SQLException e)
			{
				throw new RFHarvesterUploaderV2Exception(e);
			}
		}

		String query = "";
		Statement uploadDBStatement = null;
		String oai_identifier = null;
		String collection_id = "'" + collectionID + "'";
		String collection_name = "'" + collectionName.replaceAll("'", "''") + "'";
		String title = "'untitled'";
		String description = "NULL";
		String url = "NULL";

		String values = "";

		String out = null;

		try
		{
			if(!row.containsKey("OAI_ID")||row.get("OAI_ID")==null)
				throw new RFHarvesterUploaderV2Exception("Missing key OAI_ID");
			else
				oai_identifier = "'" + row.get("OAI_ID").get(0).replaceAll("'", "''") + "'";

			if(row.containsKey("URL")&&row.get("URL")!=null)
			{
				for(String link : row.get("URL"))
				{
					if(link.startsWith("http://"))
						url = "'" + link.replaceAll("'", "''") + "'";
				}				
			}

			if(row.containsKey("titres")&&row.get("titres")!=null)
				title = "'" + row.get("titres").get(0).replaceAll("'", "''") + "'";
			else
				throw new RFHarvesterUploaderV2Exception("Missing key titres");

			if(row.containsKey("descriptions")&&row.get("descriptions")!=null)
				description = "'" + row.get("descriptions").get(0).replaceAll("'", "''") + "'";
//			System.out.println(row.toString());

			values += oai_identifier + ", ";
			values += collection_id + ", ";
			values += collection_name + ", ";
			values += title +  ", ";
			values += description + ", ";
			values += url;

			try
			{
				query	= "INSERT INTO "
						+  tableName + "_new "
						+ "("
						+	"oai_identifier, "
						+	"collection_id, "
						+	"collection_name, "
						+	"title, "
						+	"description, "
						+	"url"
						+ ")"
						+ "VALUES"
						+ "("
						+ 	new String(values.getBytes(), "ISO-8859-1")
						+ ")";
			}
			catch(UnsupportedEncodingException e)
			{
				e.printStackTrace();
			}

			uploadDBStatement = uploadDBConnection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
			uploadDBStatement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);

			ResultSet rs = uploadDBStatement.getGeneratedKeys();
			if (rs.next())
			{
			    out = rs.getString(1);
			}
			storedRows++;
			if(storedRows >= recomandedCommit)
				commit();
			lastID = out;
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderV2Exception("Unable to insert: [" + values + "]\n                                                 Into " + tableName + "\n                                                 " + e.getClass().getName() + ": " + e.getMessage());// + "\n                                        " + query);
		}
		catch(Exception e)
		{
			String errorMess = "\n                                                 " + e.getMessage();
			errorMess +=("\n                                                 #OAI_DC ~ " + ((row.containsKey("OAI_DC"))? row.get("OAI_DC").toString() : "FAILURE"));
			errorMess +=("\n                                                 #titre ~ " + ((row.containsKey("titres"))? row.get("titres").toString() : "FAILURE"));
			errorMess +=("\n                                                 #description ~ " + ((row.containsKey("descriptions"))? row.get("descriptions").toString() : "FAILURE"));

			throw new RFHarvesterUploaderV2Exception("FATAL ERROR!!!" + errorMess);
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
		return out;
	}

	@Override
	public void end() throws RFHarvesterUploaderV2Exception
	{
		try
		{
			commit();
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderV2Exception(e);
		}
	}

	@Override
	public void confirm() throws RFHarvesterUploaderV2Exception
	{
		try
		{
			if(startedTransaction == true)
				throw new RFHarvesterUploaderV2Exception("Unable to confirm table, transaction in progress.");
			replaceOldTable();
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderV2Exception(e);
		}
	}

	public void replaceOldTable() throws SQLException
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

	protected void finalize() throws Throwable
	{

		if(uploadDBConnection != null)
		{
			uploadDBConnection.close();
		}
	}
}
