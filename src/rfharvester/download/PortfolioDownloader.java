package rfharvester.download;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import rfharvester.RFHarvesterStorageClassException;
import rfharvester.logger.RFHarvesterDatation;
import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.transformator.RFHarvesterPortfolioTransfo;
import rfharvester.upload.RFHarvesterUploadClassException;
import rfharvester.upload.RFHarvesterUploaderInterface;

public class PortfolioDownloader implements RFHarvesterDownloaderInterface
{
	private final int collection_id;
	private final int maxModifiedRecords = 100000;
	private final int sqlPushRecords = 20000;
	private final int minHarvest = 410000;
	private Connection externalDBConnection = null;
	private Connection localSqliteConnection = null;
	private Connection uploadDBConnection = null;
	private final String className = this.getClass().getName();
	private final String uploadDB = "jdbc:mysql://10.1.2.113/lf_prod";
	private final String uploadDBlogin = "root";
	private final String uploadDBpassword = "mysqlbpi";
	private final String harvestDB = "jdbc:postgresql://10.1.2.8:5432/portfoliodw";
	private final String harvestDBlogin = "rechfed";
	private final String harvestDBpassword = "EpbHI18jXk";
	private final String localDB = "jdbc:sqlite:sql/downloadtest.db";
	private final String localTableName = "dc_notices_lite";

	private HashMap<String, String> dateEndNew = new HashMap<String, String>(); // <document_types.name, primary_document_types.new_period>
	private HashMap<String, String> authorityindices = new HashMap<String, String>(); // authorityindices<indice, libelle>
	private HashMap<String, String> primarydocumenttypes = new HashMap<String, String>(); // primary_document_types<id, name>
	private HashMap<String, String> documenttypes = new HashMap<String, String>(); // documenttypes<name, primary_document_type">
	private HashSet<Integer> refsourcesLengthsReferences = new HashSet<Integer>(); // themes_references<ref_source.length> WHERE construction_mode != 'E'
	private HashMap<String, String> cduThemesReferences = new HashMap<String, String>(); // themes_references<ref_theme, name_theme>
	private HashSet<Integer> refsourcesLengthsExclusions = new HashSet<Integer>(); // themes_references<ref_source.length> WHERE construction_mode == 'E'
	private HashMap<String, String> cduThemesExclusions = new HashMap<String, String>(); // themes_references<ref_theme, name_theme>
	private HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesReferences = new HashMap<String, HashMap<String, HashMap<String, String>>>(); //themes_references<source, <ref_source, <"ref_theme"|"construction_mode", "values">>>
	private HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesExclusions = new HashMap<String, HashMap<String, HashMap<String, String>>>(); //themes_references<source, <ref_source, <"ref_theme"|"construction_mode", "values">>>

	public PortfolioDownloader(int collectionId)
	{
		RFHarvesterLogger.info("Initializing " + className);
		collection_id = collectionId;

		String jdbc = "Fatal error NOJDBC";
		String curDB = "Fatal error NODBNAME";
		try
		{
			jdbc = "org.postgresql.Driver";
			RFHarvesterLogger.debug("Init " + jdbc);
			Class.forName(jdbc);
			curDB = harvestDB;
			externalDBConnection = DriverManager.getConnection(harvestDB, harvestDBlogin, harvestDBpassword);
			RFHarvesterLogger.info("Connection with " + harvestDB + " established");

			jdbc = "org.sqlite.JDBC";
			RFHarvesterLogger.debug("Init " + jdbc);
			Class.forName(jdbc);
			curDB = localDB;
			localSqliteConnection = DriverManager.getConnection(localDB);
			RFHarvesterLogger.info("Connection with " + localDB + " established");

			jdbc = "com.mysql.jdbc.Driver";
			RFHarvesterLogger.debug("Init " + jdbc);
			Class.forName(jdbc);
			curDB = uploadDB;
			uploadDBConnection = DriverManager.getConnection(uploadDB, uploadDBlogin, uploadDBpassword);
			RFHarvesterLogger.info("Connection with " + uploadDB + " established");
		}
		catch(ClassNotFoundException e)
		{
			RFHarvesterLogger.error(jdbc + " not found" + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with missing library.
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to establish connection with " + curDB + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with a missing library.
		}
	}

	/**
	 * Grab initialization data.
	 * @param localSqliteStatement
	 */
	private void downloadPart0(Statement localSqliteStatement)
	{
		String query = "";
		ResultSet initializationData;

		Statement externalDBStatement = null;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			externalDBStatement = externalDBConnection.createStatement(type, mode);
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			//Check external DB version.
			RFHarvesterLogger.debug("JDBC verification:");
			ResultSet externalDBversion = externalDBStatement.executeQuery("SELECT VERSION() AS dbversion");
			while(externalDBversion.next())
			{
				RFHarvesterLogger.debug(externalDBversion.getString("dbversion"));
			}
			externalDBversion.close();

			query = "SELECT CONVERT(CAST(CONVERT(document_types.name USING LATIN1) AS BINARY) USING UTF8) AS new_period_name, primary_document_types.new_period AS new_period FROM primary_document_types,document_types WHERE primary_document_types.name != 'None' AND document_types.collection_id = " + collection_id + " AND primary_document_types.id = document_types.primary_document_type";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				dateEndNew.put(initializationData.getString("new_period_name"), initializationData.getString("new_period_name"));
			}
			RFHarvesterLogger.info(dateEndNew.size() + " date end new");
			initializationData.close();

			query = "SELECT indice, libelle FROM dw_authorityindices";
			RFHarvesterLogger.debug(query);
			initializationData = externalDBStatement.executeQuery(query);
			int duplicate = 0;
			while(initializationData.next())
			{
				if(authorityindices.containsKey(initializationData.getString("indice")))
				{
					RFHarvesterLogger.warning("Duplicated Authority Indice found :\n                                        Original  : " + initializationData.getString("indice") + " ~ " + authorityindices.get(initializationData.getString("indice")) +"\n                                        Duplicate : " + initializationData.getString("indice") + " ~ " + initializationData.getString("libelle"));
					duplicate++;
				}
				else
				{
					if(initializationData.getString("libelle") == null)
						authorityindices.put(initializationData.getString("indice"), null);
					else
						authorityindices.put(initializationData.getString("indice"), initializationData.getString("libelle"));					
				}
			}
			if(duplicate>0)
				RFHarvesterLogger.warning(duplicate + " duplicates found");
			RFHarvesterLogger.info(authorityindices.size() + " authority indices");
			initializationData.close();

			query = "SELECT id, CONVERT(CAST(CONVERT(name USING LATIN1) AS BINARY) USING UTF8) AS name FROM primary_document_types";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				primarydocumenttypes.put(initializationData.getString("id"), initializationData.getString("name"));
			}
			RFHarvesterLogger.info(primarydocumenttypes.size() + " primary document types");
			initializationData.close();

			query = "SELECT CONVERT(CAST(CONVERT(name USING LATIN1) AS BINARY) USING UTF8) AS name, primary_document_type FROM document_types WHERE collection_id = " + collection_id;
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				documenttypes.put(initializationData.getString("name"), initializationData.getString("primary_document_type"));
			}
			RFHarvesterLogger.info(documenttypes.size() + " document types");
			initializationData.close();


			//themes<reference, name_to_root>
			HashMap<String, String> themes = new HashMap<String, String>();
			HashMap<String, String> themesrefparent = new HashMap<String, String>();
			HashMap<String, String> themesreflabel = new HashMap<String, String>();

			String THEME_SEPARATOR = " > ";

			query = "SELECT reference, parent, CONVERT(CAST(CONVERT(label USING LATIN1) AS BINARY) USING UTF8) AS label FROM themes";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			duplicate = 0;
			while(initializationData.next())
			{
				String parent = initializationData.getString("parent").trim();
				if(parent==null || parent.length()<=0 || parent.compareTo("0") == 0)
					parent = "0";
				if(themesrefparent.containsKey(initializationData.getString("reference")))
				{
					RFHarvesterLogger.warning("Duplicated Parent Reference Theme found :\n                                        Original  : " + initializationData.getString("reference") + " ~ " + themesrefparent.get(initializationData.getString("reference")) +"\n                                        Duplicate : " + initializationData.getString("reference") + " ~ " + parent);
					duplicate++;
				}
				else
				{
					themesreflabel.put(initializationData.getString("reference"), initializationData.getString("label"));
					themesrefparent.put(initializationData.getString("reference"), parent);
				}
			}
			initializationData.close();
			RFHarvesterLogger.info(themesreflabel.size() + " reflabel");
			RFHarvesterLogger.info(themesrefparent.size() + " refparent");

			//For each references create a correspondence HashMap between reference and name_to_root
			for(String S : themesrefparent.keySet())
			{
				String parlist = themesreflabel.get(S);
				String parent = themesrefparent.get(S);
				while(parent.compareTo("0")!=0)
				{
					parlist = themesreflabel.get(parent) + THEME_SEPARATOR + parlist;
					parent = themesrefparent.get(parent);
				}
				themes.put(S, parlist);
			}

			RFHarvesterLogger.info("Begin cduTranslationInfo recuperation");
			query = "SELECT CONVERT(CAST(CONVERT(ref_source USING LATIN1) AS BINARY) USING UTF8) AS ref_source, ref_theme FROM themes_references WHERE source = 'portfoliodw' AND construction_mode != 'E'";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			duplicate = 0;
			while(initializationData.next())
			{
				String ref_source = initializationData.getString("ref_source");
				refsourcesLengthsReferences.add(ref_source.length());
				if(cduThemesReferences.containsKey(ref_source))
				{
					RFHarvesterLogger.warning("Duplicated ref_source : " + ref_source + " ~ " + themes.get(initializationData.getString("ref_theme")) + "\n                                                                   and : " + ref_source + " ~ " + cduThemesReferences.get(ref_source));
					duplicate++;
					continue;
				}
				cduThemesReferences.put(ref_source, themes.get(initializationData.getString("ref_theme")));
			}
			RFHarvesterLogger.info(cduThemesReferences.size() + " cduThemesReferences");
			if(duplicate>0)
				RFHarvesterLogger.warning(duplicate + " duplicates cduThemesReferences found");
			initializationData.close();
			

			query = "SELECT CONVERT(CAST(CONVERT(ref_source USING LATIN1) AS BINARY) USING UTF8) AS ref_source, ref_theme FROM themes_references WHERE source = 'portfoliodw' AND construction_mode = 'E'";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			duplicate = 0;
			while(initializationData.next())
			{
				String ref_source = initializationData.getString("ref_source");
				refsourcesLengthsExclusions.add(ref_source.length());
				if(cduThemesExclusions.containsKey(ref_source))
				{
					RFHarvesterLogger.warning("Duplicated ref_source : " + ref_source + " ~ " + themes.get(initializationData.getString("ref_theme")) + "\n                                                                   and : " + ref_source + " ~ " + cduThemesExclusions.get(ref_source));
					duplicate++;
					continue;
				}
				cduThemesExclusions.put(ref_source, themes.get(initializationData.getString("ref_theme")));
			}
			RFHarvesterLogger.info(cduThemesExclusions.size() + " cduThemesExclusions");
			if(duplicate>0)
				RFHarvesterLogger.warning(duplicate + " duplicates cduThemesExclusions found");
			initializationData.close();

			RFHarvesterLogger.info("Begin bdmTranslationInfoReferences recuperation");
			query = "SELECT DISTINCT source FROM themes_references WHERE construction_mode != 'E'";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				bdmThemesReferences.put(initializationData.getString("source"), null);
			}
			RFHarvesterLogger.info(bdmThemesReferences.size() + " sources found (including portfolio)");
			initializationData.close();

			for(String source : bdmThemesReferences.keySet())
			{
				RFHarvesterLogger.info("Begin bdmTranslationInfoReferences recuperation for source '" + source + "'");
				HashMap<String, HashMap<String, String>> bdmThemesReferencesPerSource = new HashMap<String, HashMap<String, String>>();
				query = "SELECT ref_source, construction_mode, ref_theme FROM themes_references WHERE construction_mode != 'E' AND source = '" + source +"'";
				RFHarvesterLogger.debug(query);
				initializationData = uploadDBStatement.executeQuery(query);
				duplicate = 0;
				while(initializationData.next())
				{
					String ref_source = initializationData.getString("ref_source");
					if(bdmThemesReferencesPerSource.containsKey(ref_source))
					{
						RFHarvesterLogger.warning("Duplicated ref_source : " + ref_source + " ~ " + themes.get(initializationData.getString("ref_theme")) + "\n                                                                   and : " + ref_source + " ~ " + bdmThemesReferencesPerSource.get(ref_source).get("name_theme"));
						duplicate++;
						continue;
					}
					HashMap<String, String> bdmThemesReferencesPerRefSource = new HashMap<String, String>();
					bdmThemesReferencesPerRefSource.put("construction_mode", initializationData.getString("construction_mode"));
					bdmThemesReferencesPerRefSource.put("name_theme", themes.get(initializationData.getString("ref_theme")));
					bdmThemesReferencesPerSource.put(ref_source, bdmThemesReferencesPerRefSource);
				}
				RFHarvesterLogger.info(bdmThemesReferencesPerSource.size() + " bdmThemesReferencesPerSource");
				if(duplicate>0)
					RFHarvesterLogger.warning(duplicate + " duplicated references found in source '" + source + "'");
				initializationData.close();
				bdmThemesReferences.put(source, bdmThemesReferencesPerSource);
			}

			RFHarvesterLogger.info("Begin bdmTranslationInfoExclusions recuperation");
			query = "SELECT DISTINCT source FROM themes_references WHERE construction_mode = 'E'";
			RFHarvesterLogger.debug(query);
			initializationData = uploadDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				bdmThemesExclusions.put(initializationData.getString("source"), null);
			}
			RFHarvesterLogger.info(bdmThemesExclusions.size() + " bdmThemesExclusions");
			initializationData.close();

			for(String source : bdmThemesExclusions.keySet())
			{
				RFHarvesterLogger.info("Begin bdmTranslationInfoExclusions recuperation for source '" + source + "'");
				HashMap<String, HashMap<String, String>> bdmThemesExclusionsPerSource = new HashMap<String, HashMap<String, String>>();
				query = "SELECT ref_source, construction_mode, ref_theme FROM themes_references WHERE construction_mode = 'E' AND source = '" + source +"'";
				RFHarvesterLogger.debug(query);
				initializationData = uploadDBStatement.executeQuery(query);
				duplicate = 0;
				while(initializationData.next())
				{
					String ref_source = initializationData.getString("ref_source");
					if(bdmThemesExclusionsPerSource.containsKey(ref_source))
					{
						RFHarvesterLogger.warning("Duplicated ref_source : " + ref_source + " ~ " + themes.get(initializationData.getString("ref_theme")) + "\n                                                                   and : " + ref_source + " ~ " + bdmThemesExclusionsPerSource.get(ref_source).get("name_theme"));
						duplicate++;
						continue;
					}
					HashMap<String, String> bdmThemesReferencesPerRefSource = new HashMap<String, String>();
					bdmThemesReferencesPerRefSource.put("construction_mode", initializationData.getString("construction_mode"));
					bdmThemesReferencesPerRefSource.put("name_theme", themes.get(initializationData.getString("ref_theme")));
					bdmThemesExclusionsPerSource.put(ref_source, bdmThemesReferencesPerRefSource);
				}
				RFHarvesterLogger.info(bdmThemesExclusionsPerSource.size() + " bdmThemesExclusionsPerSource");
				if(duplicate>0)
					RFHarvesterLogger.warning(duplicate + " duplicated exclusions found in source '" + source + "'");
				initializationData.close();
				bdmThemesExclusions.put(source, bdmThemesExclusionsPerSource);
			}
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
				if(externalDBStatement != null)
					externalDBStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * Part 1 : Initiate and fill the local SQLite database with portfolio's
	 * data
	 * 
	 * @param millis
	 * @param localSqliteStatement
	 */
	private void downloadPart1(long millis, Statement localSqliteStatement)
	{
		SimpleDateFormat portfolioDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int modifiedRecords = 0;
		int i = 0;
		int noticesCount = 0;
		String query = "";
		String dateString = "";
		String selectedIds = "";
		String createdTableColumns = "";
		ResultSet harvestedNotices;

		Statement externalDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			externalDBStatement = externalDBConnection.createStatement(type, mode);

			//Initialize local database
			//Get dc_notices columns names
			ArrayList<String> dcnoticesColumns = new ArrayList<String>();
			ResultSet dcnColumns = externalDBConnection.getMetaData().getColumns("portfoliodw", null, "dc_notices", null);
			String createdTableColumnsWithType = "";
			while(dcnColumns.next())
			{
				dcnoticesColumns.add(dcnColumns.getString(4));
				createdTableColumnsWithType += (dcnColumns.getString(4) + " " + dcnColumns.getString(6) + ", ");
			}
			dcnColumns.close();
			createdTableColumnsWithType = createdTableColumnsWithType.substring(0, createdTableColumnsWithType.length() - 2);
			RFHarvesterLogger.debug("dc_notices columns : " + dcnoticesColumns);
			//Reset local database
			String sqlite = "DROP TABLE IF EXISTS " + localTableName;
			RFHarvesterLogger.debug(sqlite);
			localSqliteStatement.execute(sqlite);
//			localSqliteStatement.execute("PRAGMA encoding = 'UTF-8'");

			//Create a new table based on dc_notice
			sqlite = "CREATE TABLE " + localTableName + " (" + createdTableColumnsWithType + ")";
			RFHarvesterLogger.debug(sqlite);
			localSqliteStatement.execute(sqlite);

			//Check if partial or full harvest
			if(millis > 0)
			{
				RFHarvesterLogger.info("Start partial harvest since: " + RFHarvesterDatation.getDateHour(millis));
				dateString = (" WHERE time > '" + portfolioDateFormat.format(millis) + "'");

				//Count modified notices to harvest
				ResultSet dcnoticesflagscount = externalDBStatement.executeQuery("SELECT COUNT(*) AS rowcount FROM dc_notices_flags" + dateString);
				while(dcnoticesflagscount.next())
				{
					if((++i) != 1) //If COUNT(*) ResultSet's size !=1 then error
					{
						RFHarvesterLogger.error("Line " + Thread.currentThread().getStackTrace()[1].getLineNumber() + " : " + i + " result in SELECT COUNT(*) ResutlSet");
					}
					modifiedRecords = dcnoticesflagscount.getInt("rowcount");
				}
				dcnoticesflagscount.close();

				RFHarvesterLogger.info(modifiedRecords + " selected notices in dc_notices_flags");
				if(modifiedRecords > maxModifiedRecords) //If more than maxModifiedRecords notices, don't set selectedIds list and do full harvest
				{
					RFHarvesterLogger.warning("More than " + maxModifiedRecords + " modified records");
					RFHarvesterLogger.info("Start full harvest");
				}
				else if(modifiedRecords <= 0) //If no modified records since the specified date, cancel the harvest
				{
					RFHarvesterLogger.warning("No modified records - Abord harvesting");
					return;
				}
				else
				{
					query = "SELECT dc_identifier AS id FROM dc_notices_flags" + dateString;
					RFHarvesterLogger.debug(query);
					ResultSet dcnoticesflagsidentifiers = externalDBStatement.executeQuery(query);
					dcnoticesflagsidentifiers.next();
					selectedIds += dcnoticesflagsidentifiers.getString("id");
					while(dcnoticesflagsidentifiers.next())
					{
						selectedIds += "," + dcnoticesflagsidentifiers.getString("id");
					}
					dcnoticesflagsidentifiers.close();
					selectedIds = " WHERE dc_identifier IN (" + selectedIds + ")";
				}
			}
			else
			{
				RFHarvesterLogger.info("Start full harvest");
			}

			//Count notices to harvest
			query = "SELECT COUNT(*) AS rowcount FROM dc_notices" + selectedIds;
			RFHarvesterLogger.debug(query);
			ResultSet rsCount = externalDBStatement.executeQuery(query);
			i = 0;
			while(rsCount.next())
			{
				if((++i) != 1) //If COUNT(*) ResultSet's size !=1 then error
				{
					RFHarvesterLogger.error("Line " + Thread.currentThread().getStackTrace()[1].getLineNumber() + " : " + i + " results in SELECT COUNT(*) ResutlSet");
				}
				noticesCount = rsCount.getInt("rowcount");
				RFHarvesterLogger.info(noticesCount + " selected notices in dc_notices");
			}
			rsCount.close();
			int totalTransactions = ((noticesCount / sqlPushRecords) + 1);
			RFHarvesterLogger.info(totalTransactions + " transactions of " + sqlPushRecords + " notices are gonna be commited into local database");
			int totalTransactionsDecimals = 0;
			for(int t = totalTransactions; t > 0; t /= 10, totalTransactionsDecimals++);

			for(String column : dcnoticesColumns)
			{
				createdTableColumns += (column + ", ");
			}
			createdTableColumns = createdTableColumns.substring(0, createdTableColumns.length() - 2);
			int j = 0;
			localSqliteStatement.execute("BEGIN TRANSACTION");
			//Select notices to harvest
			query = "SELECT * FROM dc_notices" + selectedIds;
			RFHarvesterLogger.debug(query);
			harvestedNotices = externalDBStatement.executeQuery(query);
			for(i = 1; harvestedNotices.next(); i++)
			{
				String vals = "";

				for(String column : dcnoticesColumns)
				{
					if(harvestedNotices.getString(column) == null)
						vals += ("null, ");
					else
						vals += ("'" + harvestedNotices.getString(column).replace("'", "''") + "'" + ", ");
				}
				vals = vals.substring(0, vals.length() - 2);
				localSqliteStatement.execute("INSERT INTO " + localTableName + " (" + createdTableColumns + ") VALUES(" + vals + ")");

				//Every sqlPushRecords notices, or if it's the last notice, push a transaction
				if(i % sqlPushRecords == 0 || harvestedNotices.isLast())
				{
					localSqliteStatement.execute("COMMIT");
					j++;
					RFHarvesterLogger.info(String.format("Transaction %" + totalTransactionsDecimals + "d on %d commited - %3d%c done", j, totalTransactions, (int) (((float) i / (float) noticesCount) * 100), '%'));
					if(!harvestedNotices.isLast())
					{
						localSqliteStatement.execute("BEGIN TRANSACTION");
					}
					else
					{
						RFHarvesterLogger.info("Last transaction commited with no error.");
					}
				}
			}
			harvestedNotices.close();
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
				if(externalDBStatement != null)
					externalDBStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private void downloadPart2(ArrayList<RFHarvesterUploaderInterface> uploadsList, Statement localSqliteStatement)
	{
	}

	private void downloadPart3(ArrayList<RFHarvesterUploaderInterface> uploadsList, Statement localSqliteStatement)
	{
		String query = "";
		int i = 0;
		int transfoFailCount = 0;
		try
		{
			ArrayList<String> dcnoticesColumns = new ArrayList<String>();
			ResultSet dcnColumns = localSqliteConnection.getMetaData().getColumns(null, null, localTableName, null);
			while(dcnColumns.next())
			{
				dcnoticesColumns.add(dcnColumns.getString(4));
			}
			dcnColumns.close();

			//Count notices to harvest
			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName;
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " LIMIT 40000";
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " LIMIT 400000  OFFSET 200000";
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " WHERE dc_identifier = '1309585' ";
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " WHERE dc_identifier = '331752' OR dc_identifier = '331753' OR dc_identifier = '331754'";
			RFHarvesterLogger.debug(query);
			ResultSet rsCount = localSqliteStatement.executeQuery(query);
			int noticesCount = 0;
			while(rsCount.next())
			{
				if((++i) != 1) //If COUNT(*) ResultSet's size !=1 then error
				{
					RFHarvesterLogger.error("Line " + Thread.currentThread().getStackTrace()[1].getLineNumber() + " : " + i + " results in SELECT COUNT(*) ResutlSet");
				}
				noticesCount = rsCount.getInt("rowcount");
				RFHarvesterLogger.info(noticesCount + " selected notices in " + localTableName);
			}
			rsCount.close();

			HashMap<String, Integer> uploadDecimals = new HashMap<String, Integer>();
			HashMap<String, Integer> uploadTotalTransaction = new HashMap<String, Integer>();
			HashMap<String, Integer> uploadTransaction = new HashMap<String, Integer>();
			HashMap<String, Integer> uploadErrors = new HashMap<String, Integer>();

			for(RFHarvesterUploaderInterface upload : uploadsList)
			{
				int totalTransations = ((noticesCount / upload.getRecomandedCommit()) + 1);
				RFHarvesterLogger.info(totalTransations + " transactions of " + upload.getRecomandedCommit() + " notices are gonna be commited through " + upload.getClassName());
				int totalTransationsDecimals = 0;
				for(int t = totalTransations; t > 0; t /= 10, totalTransationsDecimals++);
				uploadDecimals.put(upload.getClassName(), totalTransationsDecimals);
				uploadTotalTransaction.put(upload.getClassName(), totalTransations);
				uploadTransaction.put(upload.getClassName(), 0);
				uploadErrors.put(upload.getClassName(), 0);
			}

			query = "SELECT * FROM " + localTableName;
//			query = "SELECT * FROM " + localTableName + " LIMIT 100000";
//			query = "SELECT * FROM " + localTableName + " LIMIT 400000  OFFSET 200000";
//			query = "SELECT * FROM " + localTableName + " WHERE dc_identifier = '1309585'";
//			query = "SELECT * FROM " + localTableName + " WHERE dc_identifier = '331752' OR dc_identifier = '331753' OR dc_identifier = '331754'";
			ResultSet storedNotices = localSqliteStatement.executeQuery(query);
			RFHarvesterUploaderInterface volumesInterface = null;
			for(RFHarvesterUploaderInterface Upload : uploadsList)
			{
				Upload.begin();
				if(Upload.getClassName().compareTo("rfharvester.upload.UploadVolumesMySQL") == 0)
					volumesInterface = Upload;
			}
			i = 0;

			while(storedNotices.next())
			{
				RFHarvesterPortfolioTransfo newRow = new RFHarvesterPortfolioTransfo();
				//Fill a RFHarvesterStorageClass with local database data
				for(String column : dcnoticesColumns)
				{
					newRow.put(column, storedNotices.getString(column));
				}

				try
				{
					i++;
					newRow.transform(collection_id, volumesInterface, dateEndNew, authorityindices, documenttypes, primarydocumenttypes, refsourcesLengthsReferences, cduThemesReferences, refsourcesLengthsExclusions, cduThemesExclusions, bdmThemesReferences, bdmThemesExclusions);

					//Send the new row to the different uploads
					final RFHarvesterPortfolioTransfo finalNewRow = newRow;
					final int finalI = i;
					final int finalNoticesCount = noticesCount;
					final HashMap<String, Integer> finalUploadDecimals = uploadDecimals;
					final HashMap<String, Integer> finalUploadTransaction = uploadTransaction;
					final HashMap<String, Integer> finalTotalTransaction = uploadTotalTransaction;
					final HashMap<String, Integer> finalUploadErrors = uploadErrors;

//					ExecutorService es = Executors.newCachedThreadPool();

					ArrayList<RFHarvesterUploaderInterface> revertUploadsList = new ArrayList<>();
					for(final RFHarvesterUploaderInterface Upload : uploadsList)
					{
//						System.out.println("aaa");
//						revertUploadsList = new ArrayList<>();
//						es.execute(new Runnable()
//						{
//							public void run()
//							{
								String upClassName = Upload.getClassName();
								try
								{
									if(Upload.getClassName().compareTo("rfharvester.upload.UploadVolumesMySQL") != 0)
										Upload.insertRow(finalNewRow);
									if(Upload.getRecomandedCommit() > 0 && Upload.getStoredRows() >= Upload.getRecomandedCommit())
									{
										int upDecimals = finalUploadDecimals.get(upClassName);
										int upTransaction = finalUploadTransaction.get(upClassName) + 1;
										finalUploadTransaction.put(upClassName, upTransaction);
										int upTotalTransaction = finalTotalTransaction.get(upClassName);
										Upload.commit();
										RFHarvesterLogger.info(String.format("Transaction %" + upDecimals + "d on %d commited for " + upClassName + " - %3d%c done", upTransaction, upTotalTransaction, (int) (((float) finalI / (float) finalNoticesCount) * 100), '%'));
										Upload.begin();
									}
									revertUploadsList.add(Upload);
//									System.out.println("revertUploadsList : " + revertUploadsList.size());
								}
								catch(RFHarvesterUploadClassException e)
								{
									int errorTransaction = finalUploadErrors.get(upClassName) + 1;
									finalUploadErrors.put(upClassName, errorTransaction);
									RFHarvesterLogger.error(e.getMessage());
//									System.out.println("errorTransaction : " + errorTransaction);
//									System.out.println("revertUploadsList : " + revertUploadsList.size());
									for(final RFHarvesterUploaderInterface revertUpload : revertUploadsList)
										revertUpload.storeErrorIdentifier(finalNewRow.get("dc_identifier"));
									break;
//									e.printStackTrace();
								}
//							}
//						});
					}
//					es.shutdown();
//					try
//					{
//						while(!es.awaitTermination(1, TimeUnit.SECONDS));
//					}
//					catch(InterruptedException e)
//					{
//						e.printStackTrace();
//						System.exit(0);
//					}
//					System.out.println("bbb");
				}
				catch(RFHarvesterStorageClassException e)
				{
					transfoFailCount++;
					RFHarvesterLogger.error(e.getMessage());
					//e.printStackTrace();
				}
			}
			ExecutorService es = Executors.newCachedThreadPool();
			for(final RFHarvesterUploaderInterface Upload : uploadsList)
			{
				es.execute(new Runnable()
				{
					public void run()
					{
						RFHarvesterLogger.info("Last transaction commited for " + Upload.getClassName());
						Upload.commit();
					}
				});
			}
			es.shutdown();
			try
			{
				while(!es.awaitTermination(1, TimeUnit.SECONDS));
			}
			catch(InterruptedException e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			storedNotices.close();
			RFHarvesterLogger.info(i + " rows sent.");

			RFHarvesterState.updateHarvestedDocuments(i);
			if(i<minHarvest)
			{
				RFHarvesterLogger.error("Less than " + minHarvest + " harvested notices.\n                                                 Harvest canceled.");
				RFHarvesterState.updateStatus("ERREUR!!!");
				System.exit(0);
			}
			if(transfoFailCount != 0)
				RFHarvesterLogger.warning(transfoFailCount + " on " + i + " rows transformation failed.");
			else
				RFHarvesterLogger.info("Transformations completed with no errors.");

			boolean clean = false;
			for(RFHarvesterUploaderInterface Upload : uploadsList)
			{
				String upClassName = Upload.getClassName();
				int errorTransaction = uploadErrors.get(upClassName);
				if(errorTransaction != 0)
				{
					RFHarvesterLogger.warning(upClassName + " : " + errorTransaction + " on  " + (i - transfoFailCount) + " rows failed to be uploaded");
					clean = true;
				}
				else
					RFHarvesterLogger.info(upClassName + " : " + "Upload completed with no errors");

			}
			if(clean)
			{
				RFHarvesterLogger.info("Start cleaning upload errors");
				for(final RFHarvesterUploaderInterface Upload : uploadsList)
				{
					Upload.cleanErrors();
				}
				RFHarvesterLogger.info("Cleaning finished");
			}
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void download(ArrayList<RFHarvesterUploaderInterface> uploadsList, long millis)
	{
		//Created statements for local and external connections
		Statement localSqliteStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			localSqliteStatement = localSqliteConnection.createStatement(type, mode);
			RFHarvesterLogger.info(className + " Download Phase 0");
			downloadPart0(localSqliteStatement);
			RFHarvesterLogger.info(className + " Download Phase 1");
			downloadPart1(millis, localSqliteStatement);
			RFHarvesterLogger.info(className + " Download Phase 2");
			downloadPart2(uploadsList, localSqliteStatement);
			RFHarvesterLogger.info(className + " Download Phase 3");
			downloadPart3(uploadsList, localSqliteStatement);
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
				if(localSqliteStatement != null)
					localSqliteStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close local database statement:\n                                        " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void finalize() throws Throwable
	{
		super.finalize();
		if(externalDBConnection != null)
		{
			externalDBConnection.close();
		}
		if(localSqliteConnection != null)
		{
			localSqliteConnection.close();
		}
		if(uploadDBConnection != null)
		{
			uploadDBConnection.close();
		}
	}
}
