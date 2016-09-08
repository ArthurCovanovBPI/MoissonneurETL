package bpi.replication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLReplicator
{
	private int waitingTime = 5;	//Wait 5 seconds before connection time out
	private Connection uploadDBConnection = null;
//	private String DataBase;
//	private String DBlogin;
//	private String DBpassword;

	public MySQLReplicator(String DB, String login, String password) throws ClassNotFoundException, SQLException
	{
//		DataBase=DB;
//		DBlogin=login;
//		DBpassword=password;
		Class.forName("com.mysql.jdbc.Driver");
		uploadDBConnection = DriverManager.getConnection(DB, login, password);
	}

	/**
	 * @return True if replication is started. False in other case.
	 * @throws InterruptedException Thread exception
	 * @throws TestReplicationException Replication error detected
	 */
	public boolean replicationStatus() throws InterruptedException, MySQLReplicatorException
	{
		String query;
		Statement uploadDBStatement = null;
		boolean result = false;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "SHOW SLAVE STATUS";
			for(int i=0; i<waitingTime; i++)
			{
				ResultSet replicationState = uploadDBStatement.executeQuery(query);
				String Slave_IO_Running = null;
				String Slave_SQL_Running = null;
				for(int j=0;replicationState.next();j++)
				{
					if(j>0)
						throw new MySQLReplicatorException("More than 1 replication state");
					Slave_IO_Running = replicationState.getString("Slave_IO_Running");
					Slave_SQL_Running = replicationState.getString("Slave_SQL_Running");
//					System.out.println("Slave_IO_Running - " + Slave_IO_Running);
//					System.out.println("Slave_SQL_Running - " + Slave_SQL_Running);
				}
				if(Slave_IO_Running.compareTo("Connecting")==0 || Slave_SQL_Running.compareTo("Connecting")==0)
				{
					if(i>=(waitingTime-1))
						throw new MySQLReplicatorException("Replication connection timed out");
					Thread.sleep(1000);
					continue;
				}
				if(Slave_IO_Running.compareTo(Slave_SQL_Running)!=0)
				{
					throw new MySQLReplicatorException("Slave_IO_Running!=Slave_SQL_Running\nSlave_IO_Running = " + Slave_IO_Running + "\nSlave_SQL_Running = " + Slave_SQL_Running);
				}
				if(Slave_IO_Running.compareTo("Yes")==0)
					result = true;
				break;
			}
		}
		catch(SQLException e)
		{
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
				e.printStackTrace();
			}
		}
		return result;
	}

	public void printSlaveStatus()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "SHOW SLAVE STATUS";
			ResultSet replicationState = uploadDBStatement.executeQuery(query);
			while(replicationState.next())
			{
				ResultSetMetaData columns = replicationState.getMetaData();
				for(int i = 1; i<=columns.getColumnCount(); i++)
					System.out.println(columns.getColumnName(i) + " - " + replicationState.getString(columns.getColumnName(i)));
			}
			replicationState.close();
		}
		catch(SQLException e)
		{
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
				e.printStackTrace();
			}
		}
	}

	private void stopstartReplication(String StopStart)
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			if(StopStart.compareTo("STOP")==0 || StopStart.compareTo("START")==0)
			{
				query = StopStart + " SLAVE";
				uploadDBStatement.execute(query);
			}
		}
		catch(SQLException e)
		{
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
				e.printStackTrace();
			}
		}
	}

	public void stopReplication() throws MySQLReplicatorException, InterruptedException
	{
		stopstartReplication("STOP");
		if(replicationStatus())
			throw new MySQLReplicatorException("Unable to stop replication");
	}
	
	public void startReplication() throws MySQLReplicatorException, InterruptedException
	{
		stopstartReplication("START");
		if(!replicationStatus())
			throw new MySQLReplicatorException("Unable to start replication");
	}
}
