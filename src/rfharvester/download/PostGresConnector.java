package rfharvester.download;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostGresConnector
{
	private Connection externalDBConnection = null;
	private final String harvestDB = "jdbc:postgresql://10.1.2.8:5432/portfoliodw";
	private final String harvestDBlogin = "rechfed";
	private final String harvestDBpassword = "EpbHI18jXk";
	public PostGresConnector(int collectionId)
	{
		String jdbc = "Fatal error NOJDBC";
		String curDB = "Fatal error NODBNAME";
		try
		{
			jdbc = "org.postgresql.Driver";
			System.out.println("DEBUG : Init " + jdbc);
			Class.forName(jdbc);
			curDB = harvestDB;
			externalDBConnection = DriverManager.getConnection(harvestDB, harvestDBlogin, harvestDBpassword);
			System.out.println("INFO : Connection with " + harvestDB + " established");
		}
		catch(ClassNotFoundException e)
		{
			System.out.println("ERROR : " + jdbc + " not found" + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with missing library.
		}
		catch(SQLException e)
		{
			System.out.println("ERROR : Unable to establish connection with " + curDB + "\n                                        " + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with a missing library.
		}
	}

	/**
	 * Grab initialization data.
	 * @param localSqliteStatement
	 */
	@SuppressWarnings("unused")
	private void downloadExample()
	{
		String query = "";
		ResultSet initializationData;

		Statement externalDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			externalDBStatement = externalDBConnection.createStatement(type, mode);

			//Check external DB version.
			System.out.println("DEBUG : JDBC verification:");
			ResultSet externalDBversion = externalDBStatement.executeQuery("SELECT VERSION() AS dbversion");
			while(externalDBversion.next())
			{
				System.out.println("DEBUG : " + externalDBversion.getString("dbversion"));
			}
			externalDBversion.close();

			query = "SELECT indice, libelle FROM dw_authorityindices";
			System.out.println("DEBUG : " + query);
			initializationData = externalDBStatement.executeQuery(query);
			while(initializationData.next())
			{
				if(initializationData.getString("column") == null);
			}
			initializationData.close();

		}
		catch(SQLException e)
		{
			System.out.println("ERROR : " + e.getClass().getName() + ": " + e.getMessage());
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
				System.out.println("ERROR : Unable to close external database statement: " + e.getMessage());
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
	}
}
