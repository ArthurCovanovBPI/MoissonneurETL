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
import rfharvester.utils.RFHarvesterUtilities;

public class UploadMetadatasMySQL5V2 implements RFHarvesterUploaderV2Interface
{
	private final String uploadDB;
	private final String uploadDBlogin = "root";
	private final String uploadDBpassword = "mysqlbpi";
	private final String tableName = "metadatas";

	private Connection uploadDBConnection = null;

	private final int collectionID;
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

	public UploadMetadatasMySQL5V2(String uploadDB, int recomandedCommit, int collectionID) throws ClassNotFoundException, SQLException
	{
		this.uploadDB = "jdbc:mysql://" + uploadDB + "?characterEncoding=ISO-8859-1&autoReconnect=true";

		this.collectionID = collectionID;
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
		String out = null;

		if(startedTransaction == false)
		try
		{
			begin();
		}
		catch(SQLException e)
		{
			throw new RFHarvesterUploaderV2Exception(e);
		}

		String query = "";
		Statement uploadDBStatement = null;
		
		

		String dc_identifier = null;
		String controls_id = null;
		String collection_id = "'" + collectionID + "'";

		String dc_title = RFHarvesterUtilities.arrayListToString(row.get("titres"), "; ");
		String dc_creator = RFHarvesterUtilities.arrayListToString(row.get("auteurs"), "; ");
		String dc_subject = RFHarvesterUtilities.arrayListToString(row.get("sujets"), "; ");
		String dc_description = RFHarvesterUtilities.arrayListToString(row.get("descriptions"), "; ");
		String dc_publisher = RFHarvesterUtilities.arrayListToString(row.get("editeurs"), "; ");
		String dc_contributor = RFHarvesterUtilities.arrayListToString(row.get("contributeurs"), "; ");
		String dc_date = RFHarvesterUtilities.arrayListToString(row.get("dates"), "; ");
		String dc_type = RFHarvesterUtilities.arrayListToString(row.get("types"), "; ");
		if(row.containsKey("OAI_defaultDocumentType"))
		{
			dc_type = row.get("OAI_defaultDocumentType").get(0);
		}
		String dc_format = RFHarvesterUtilities.arrayListToString(row.get("formats"), "; ");
		String dc_source = RFHarvesterUtilities.arrayListToString(row.get("sources"), "; ");
		String dc_relation = RFHarvesterUtilities.arrayListToString(row.get("relations"), "; ");
		String dc_coverage = RFHarvesterUtilities.arrayListToString(row.get("coverages"), "; ");
		String dc_rights = RFHarvesterUtilities.arrayListToString(row.get("rights"), "; ");
		String dc_language = RFHarvesterUtilities.arrayListToString(row.get("langues"), "; ");
		String osu_thumbnail = RFHarvesterUtilities.arrayListToString(row.get("vignettes"), "; ");


		String values = "";

		try
		{
//			int type = ResultSet.TYPE_FORWARD_ONLY;
//			int mode = ResultSet.CONCUR_READ_ONLY;
//			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			if(row.containsKey("OAI_ID")&&row.get("OAI_ID")!=null)
				dc_identifier = "'" + row.get("OAI_ID").get(0).replaceAll("'", "''") + "'";
			else
				throw new RFHarvesterUploaderV2Exception("Missing key OAI_ID");

			if(row.containsKey("controlID") && row.get("controlID")!=null)
				controls_id = row.get("controlID").get(0);
			else
				throw new RFHarvesterUploaderV2Exception("Missing controlID in "+ row.toString());

			dc_title = (((dc_title!=null))? ("'" + dc_title.replaceAll("'", "''") + "'") : "NULL");
			dc_creator = (((dc_creator!=null))? ("'" + dc_creator.replaceAll("'", "''") + "'") : "NULL");
			dc_subject = (((dc_subject!=null))? ("'" + dc_subject.replaceAll("'", "''") + "'") : "NULL");
			dc_description = (((dc_description!=null))? ("'" + dc_description.replaceAll("'", "''") + "'") : "NULL");
			dc_publisher = (((dc_publisher!=null))? ("'" + dc_publisher.replaceAll("'", "''") + "'") : "''");
			dc_contributor = (((dc_contributor!=null))? ("'" + dc_contributor.replaceAll("'", "''") + "'") : "''");
			dc_date = (((dc_date!=null))? ("'" + dc_date.replaceAll("'", "''") + "'") : "''");
			dc_type = (((dc_type!=null))? ("'" + dc_type.replaceAll("'", "''") + "'") : "NULL");
			dc_format = (((dc_format!=null))? ("'" + dc_format.replaceAll("'", "''") + "'") : "NULL");
			dc_source = (((dc_source!=null))? ("'" + dc_source.replaceAll("'", "''") + "'") : "NULL");
			dc_relation = (((dc_relation!=null))? ("'" + dc_relation.replaceAll("'", "''") + "'") : "''");
			dc_coverage = (((dc_coverage!=null))? ("'" + dc_coverage.replaceAll("'", "''") + "'") : "''");
			dc_rights = (((dc_rights!=null))? ("'" + dc_rights.replaceAll("'", "''") + "'") : "NULL");
			dc_language = (((dc_language!=null))? ("'" + dc_language.replaceAll("'", "''") + "'") : "NULL");
			osu_thumbnail = (((osu_thumbnail!=null))? ("'" + osu_thumbnail.replaceAll("'", "''") + "'") : "NULL");

			values += collection_id + ", ";
			values += controls_id + ", ";
			values += dc_title + ", ";
			values += dc_creator + ", ";
			values += dc_subject + ", ";
			values += dc_description + ", ";
			values += dc_publisher + ", ";
			values += dc_contributor + ", ";
			values += dc_date + ", ";
			values += dc_type + ", ";
			values += dc_format + ", ";
			values += dc_source + ", ";
			values += dc_relation + ", ";
			values += dc_coverage + ", ";
			values += dc_rights + ", ";
			values += osu_thumbnail + ", ";
			values += dc_language + ", ";
			values += dc_identifier;

			try
			{
				query	= "INSERT INTO "
						+  tableName + "_new "
						+ "("
						+	"collection_id, "
						+	"controls_id, "
						+	"dc_title, "
						+	"dc_creator, "
						+	"dc_subject, "
						+	"dc_description, "
						+	"dc_publisher, "
						+	"dc_contributor, "
						+	"dc_date, "
						+	"dc_type, "
						+	"dc_format, "
						+	"dc_source, "
						+	"dc_relation, "
						+	"dc_coverage, "
						+	"dc_rights, "
						+	"osu_thumbnail, "
						+	"dc_language, "
						+	"dc_identifier"
						+ ")"
						+ "VALUES"
						+ "("
						+ 	new String(values.getBytes(), "ISO-8859-1")
						+ ")";
			}
			catch(UnsupportedEncodingException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

//			RFHarvesterLogger.debug(query);
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
			String errorMess = "";
			errorMess +=("\n                                                 #OAI_DC ~ " + ((row.containsKey("OAI_DC"))? row.get("OAI_DC").toString() : "FAILURE"));
			errorMess +=("\n                                                 #titre ~ " + ((row.containsKey("titre"))? row.get("titre").toString() : "FAILURE"));
			errorMess +=("\n                                                 #description ~ " + ((row.containsKey("description"))? row.get("description").toString() : "FAILURE"));

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
