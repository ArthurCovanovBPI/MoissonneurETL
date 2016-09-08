package rfharvester.logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import rfharvester.ExitCodes;

/**
 * Class used to write into log-files. Never instantiated. Only use:
 * RFHarvesterLogger.info("InfoText"); RFHarvesterLogger.error("ErrorText");
 * RFHarvesterLogger.debug("DebugText"); And activate or disactivate different
 * logger with getters and setters. Loggers are NULL per default, needed to be
 * set.
 * 
 * @author ArthurCovanov
 */
public abstract class RFHarvesterState
{
	private final static String statusDB = "jdbc:mysql://127.0.0.1/bpiharvester?autoReconnect=true";
	private final static String statusDBlogin = "root";
	private final static String statusDBpassword = "password";
	private static Connection statusDBConnection = null;
	private final static String tableName = "status";

	private static int ID=-1;

	public static int checkRunningStatus()
	{
		int result = 0;
		String query;
		Statement statusDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			statusDBStatement = statusDBConnection.createStatement(type, mode);

			query = "SELECT COUNT(*) AS rowcount FROM " + tableName + " WHERE status = 'En cours'";
			RFHarvesterLogger.debug(query);

			//Count modified notices to harvest
			ResultSet runningInstancesCount = statusDBStatement.executeQuery(query);
			int i = 0;
			while(runningInstancesCount.next())
			{
				if((++i) != 1) //If COUNT(*) ResultSet's size !=1 then error
				{
					RFHarvesterLogger.error("Line " + Thread.currentThread().getStackTrace()[1].getLineNumber() + " : " + i + " result in SELECT COUNT(*) ResutlSet");
				}
				result = runningInstancesCount.getInt("rowcount");
			}
			runningInstancesCount.close();
			
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
			System.exit(ExitCodes.EX_IOERR.value());
		}
		finally
		{
			try
			{
				if(statusDBStatement != null)
					statusDBStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
				e.printStackTrace();
			}
		}
		return result;
	}

	public static void begin()
	{
		if(ID != -1)
		{
			RFHarvesterLogger.error("ID already setted: " + ID);
			return;
		}
		String query;
		Statement statusDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			statusDBStatement = statusDBConnection.createStatement(type, mode);
			
			String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();

			query = "INSERT INTO " + tableName + "(PID, status) VALUES (" + Long.parseLong(processName.split("@")[0]) + ", 'En cours')";
			RFHarvesterLogger.debug(query);

			statusDBStatement = statusDBConnection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
			statusDBStatement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);

			ResultSet rs = statusDBStatement.getGeneratedKeys();
			if(rs.next())
			{
				ID = rs.getInt(1);
			}
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
			System.exit(ExitCodes.EX_IOERR.value());
		}
		finally
		{
			try
			{
				if(statusDBStatement != null)
					statusDBStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static void executeQuery(String query)
	{
		if(ID <= 0)
		{
			RFHarvesterLogger.error("Can't execute: " + query + "\n                                                 Unsetted status ID!!!");
			return;
		}
		Statement statusDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			statusDBStatement = statusDBConnection.createStatement(type, mode);

//			RFHarvesterLogger.debug(query);
			statusDBStatement.execute(query);
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
				if(statusDBStatement != null)
					statusDBStatement.close();
			}
			catch(SQLException e)
			{
				RFHarvesterLogger.error("Unable to close external database statement: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static void updateHarvestedDocuments(int harvested)
	{
		executeQuery("UPDATE " + tableName + " SET harvested=" + harvested + " WHERE ID = " + ID);
	}

	public static void updateConfiguration(String configuration)
	{
		executeQuery("UPDATE " + tableName + " SET configuration='" + configuration.replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\") + "' WHERE ID = " + ID);
	}

	public static void updateMessage(String message)
	{
		executeQuery("UPDATE " + tableName + " SET message='" + message.replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\") + "' WHERE ID = " + ID);
	}

	public static void updateStatus(String status)
	{
		executeQuery("UPDATE " + tableName + " SET status='" + status.replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\") + "' WHERE ID = " + ID);
	}

	public static void endStatus()
	{
		executeQuery("UPDATE " + tableName + " SET endDate=CURRENT_TIMESTAMP WHERE ID = " + ID);
	}

	static
	{
		RFHarvesterLogger.info("Initializing " + RFHarvesterState.class.getName());

		String jdbc = "Fatal error NOJDBC";
		String curDB = "Fatal error NODBNAME";
		try
		{
			jdbc = "com.mysql.jdbc.Driver";
			RFHarvesterLogger.info("Init " + jdbc);
			Class.forName(jdbc);
			curDB = RFHarvesterState.statusDB;
			RFHarvesterState.statusDBConnection = DriverManager.getConnection(statusDB, statusDBlogin, statusDBpassword);
			RFHarvesterLogger.info("Connection with " + statusDB + " established");
		}
		catch(ClassNotFoundException e)
		{
			RFHarvesterLogger.error(jdbc + "not found" + RFHarvesterLogger.exceptionToString(e));
			e.printStackTrace();
			
			System.exit(ExitCodes.EX_NOINPUT.value()); // Program won't run with missing library.
		}
		catch(SQLException e)
		{
			RFHarvesterLogger.error("Unable to establish connection with" + curDB + RFHarvesterLogger.exceptionToString(e));
			e.printStackTrace();
			System.exit(ExitCodes.EX_NOINPUT.value()); // Program won't run with a missing library.
		}
	}
}