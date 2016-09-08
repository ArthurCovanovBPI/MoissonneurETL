package rfharvester.download;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import rfharvester.RFHarvesterStorageClassException;
import rfharvester.logger.RFHarvesterLogger;
import rfharvester.transformator.RFHarvesterAuthoritiesTransfo;
import rfharvester.upload.RFHarvesterUploadClassException;
import rfharvester.upload.RFHarvesterUploaderInterface;

public class AuthoritiesDownloader implements RFHarvesterDownloaderInterface
{
	private final int sqlPushRecords = 20000;
	private Connection externalDBConnection = null;
	private Connection localSqliteConnection = null;
	private Connection uploadDBConnection = null;
	private final String className = this.getClass().getName();
	private final String harvestDB = "jdbc:postgresql://10.1.2.8:5432/portfoliodw";
	private final String harvestDBlogin = "rechfed";
	private final String harvestDBpassword = "EpbHI18jXk";
	private final String localDB = "jdbc:sqlite:downloadtest.db";
	private final String localTableName = "dw_authoritynotices";

	public AuthoritiesDownloader()
	{
		RFHarvesterLogger.info("Initializing " + className);

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
		int i = 0;
		int noticesCount = 0;
		String query = "";
		String createdTableColumns = "";
		ResultSet harvestedNotices;

		Statement externalDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			externalDBStatement = externalDBConnection.createStatement(type, mode);

			//Initialize local database
			//Get dw_authoritynotices columns names
			ArrayList<String> dcnoticesColumns = new ArrayList<String>();
			ResultSet dcnColumns = externalDBConnection.getMetaData().getColumns("portfoliodw", null, "dw_authoritynotices", null);
			String createdTableColumnsWithType = "";
			while(dcnColumns.next())
			{
				dcnoticesColumns.add(dcnColumns.getString(4));
				createdTableColumnsWithType += (dcnColumns.getString(4) + " " + dcnColumns.getString(6) + ", ");
			}
			dcnColumns.close();
			createdTableColumnsWithType = createdTableColumnsWithType.substring(0, createdTableColumnsWithType.length() - 2);
			RFHarvesterLogger.debug(localTableName + " columns : " + dcnoticesColumns);
			//Reset local database
			String sqlite = "DROP TABLE IF EXISTS " + localTableName;
			RFHarvesterLogger.debug(sqlite);
			localSqliteStatement.execute(sqlite);
//			localSqliteStatement.execute("PRAGMA encoding = 'UTF-8'");

			//Create a new table based on dw_authoritynotices
			sqlite = "CREATE TABLE " + localTableName + " (" + createdTableColumnsWithType + ")";
			RFHarvesterLogger.debug(sqlite);
			localSqliteStatement.execute(sqlite);

			RFHarvesterLogger.info("Start full harvest");

			//Count notices to harvest
			query = "SELECT COUNT(*) AS rowcount FROM "+ localTableName;
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
				RFHarvesterLogger.info(noticesCount + " selected notices in "+ localTableName);
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
			query = "SELECT * FROM " + localTableName;
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
			ArrayList<String> dwauthoritynoticesColumns = new ArrayList<String>();
			ResultSet dcnColumns = localSqliteConnection.getMetaData().getColumns(null, null, localTableName, null);
			while(dcnColumns.next())
			{
				dwauthoritynoticesColumns.add(dcnColumns.getString(4));
			}
			dcnColumns.close();

			//Count notices to harvest
			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName;
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " LIMIT 400000";
//			query = "SELECT COUNT(*) AS rowcount FROM " + localTableName + " WHERE dc_identifier = '167427' ";
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
//			query = "SELECT * FROM " + localTableName + " LIMIT 400000";
//			query = "SELECT * FROM " + localTableName + " WHERE dc_identifier = '167427' ";
			ResultSet storedNotices = localSqliteStatement.executeQuery(query);

			i = 0;

			while(storedNotices.next())
			{
				RFHarvesterAuthoritiesTransfo newRow = new RFHarvesterAuthoritiesTransfo();
				//Fill a RFHarvesterStorageClass with local database data
				for(String column : dwauthoritynoticesColumns)
				{
					newRow.put(column, storedNotices.getString(column));
				}

				try
				{
					i++;
					newRow.transform();
					//System.out.println(i + " ~ " + newRow.get("dc_title"));
					//System.out.println(i + " ~ " + newRow.get("title"));

					//Send the new row to the different uploads
					
					final RFHarvesterAuthoritiesTransfo finalNewRow = newRow;
					final int finalI = i;
					final int finalNoticesCount = noticesCount;
					final HashMap<String, Integer> finalUploadDecimals = uploadDecimals;
					final HashMap<String, Integer> finalUploadTransaction = uploadTransaction;
					final HashMap<String, Integer> finalTotalTransaction = uploadTotalTransaction;
					final HashMap<String, Integer> finalUploadErrors = uploadErrors;

					ExecutorService es = Executors.newCachedThreadPool();
					for(final RFHarvesterUploaderInterface Upload : uploadsList)
					{
						es.execute(new Runnable()
						{
							public void run()
							{
								String upClassName = Upload.getClassName();
								try
								{
									if(Upload.getClassName().compareTo("rfharvester.upload.UploadVolumesMySQL") != 0)
										Upload.insertRow(finalNewRow);
									if(Upload.getStoredRows() >= Upload.getRecomandedCommit())
									{
										int upDecimals = finalUploadDecimals.get(upClassName);
										int upTransaction = finalUploadTransaction.get(upClassName) + 1;
										finalUploadTransaction.put(upClassName, upTransaction);
										int upTotalTransaction = finalTotalTransaction.get(upClassName);
										Upload.commit();
										RFHarvesterLogger.info(String.format("Transaction %" + upDecimals + "d on %d commited for " + upClassName + " - %3d%c done", upTransaction, upTotalTransaction, (int) (((float) finalI / (float) finalNoticesCount) * 100), '%'));
										Upload.begin();
									}
								}
								catch(RFHarvesterUploadClassException e)
								{
									int errorTransaction = finalUploadErrors.get(upClassName) + 1;
									finalUploadErrors.put(upClassName, errorTransaction);
									RFHarvesterLogger.error(e.getMessage());
									e.printStackTrace();
								}
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
			if(transfoFailCount != 0)
				RFHarvesterLogger.warning(transfoFailCount + " on " + i + " rows transformation failed.");
			else
				RFHarvesterLogger.info("Transformations completed with no errors.");

			for(RFHarvesterUploaderInterface Upload : uploadsList)
			{
				String upClassName = Upload.getClassName();
				int errorTransaction = uploadErrors.get(upClassName);
				if(errorTransaction != 0)
					RFHarvesterLogger.warning(upClassName + " : " + errorTransaction + " on  " + (i - transfoFailCount) + " rows failed to be uploaded");
				else
					RFHarvesterLogger.info(upClassName + " : " + "Upload completed with no errors");

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
		if(uploadDBConnection != null)
		{
			uploadDBConnection.close();
		}
	}
}
