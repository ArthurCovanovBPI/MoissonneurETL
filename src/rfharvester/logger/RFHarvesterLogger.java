package rfharvester.logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.output.NullOutputStream;

/**
 * Class used to write into log-files. Never instantiated. Only use:
 * RFHarvesterLogger.info("InfoText"); RFHarvesterLogger.error("ErrorText");
 * RFHarvesterLogger.debug("DebugText"); And activate or disactivate different
 * logger with getters and setters. Loggers are NULL per default, needed to be
 * set.
 * 
 * @author ArthurCovanov
 */
public abstract class RFHarvesterLogger
{
	private final static String uploadDB = "jdbc:mysql://127.0.0.1/bpiharvester?autoReconnect=true";
	private final static String uploadDBlogin = "root";
	private final static String uploadDBpassword = "password";
	private final static String tableName = "logs";
	private static Connection uploadDBConnection = null;
	private static boolean infoLogger = true;
	private static boolean warningLogger = true;
	private static boolean errorLogger = true;
	private static boolean debugLogger = true;
	private static boolean datation = false;
	private static OutputStream infoLog;
	private static OutputStream warningLog;
	private static OutputStream errorLog;
	private static OutputStream debugLog;

	/**
	 * @return datation state.
	 */
	public static boolean isDatation()
	{
		return datation;
	}

	/**
	 * @param New datation state.
	 */
	public static void setDatation(boolean datation)
	{
		RFHarvesterLogger.datation = datation;
	}

	/**
	 * @return infoLogger state.
	 */
	public static boolean isInfoLogger()
	{
		return infoLogger;
	}

	/**
	 * @param infoLogger New infoLogger state.
	 */
	public static void setInfoLogger(boolean infoLogger)
	{
		RFHarvesterLogger.infoLogger = infoLogger;
	}

	/**
	 * @return warningLogger state.
	 */
	public static boolean isWarningLogger()
	{
		return warningLogger;
	}

	/**
	 * @param warningLogger New warningLogger state.
	 */
	public static void setWarningLogger(boolean warningLogger)
	{
		RFHarvesterLogger.warningLogger = warningLogger;
	}

	/**
	 * @return errorLogger state.
	 */
	public static boolean isErrorLogger()
	{
		return errorLogger;
	}

	/**
	 * @param errorLogger New errorLogger state.
	 */
	public static void setErrorLogger(boolean errorLogger)
	{
		RFHarvesterLogger.errorLogger = errorLogger;
	}

	/**
	 * @return debugLogger state.
	 */
	public static boolean isDebugLogger()
	{
		return debugLogger;
	}

	/**
	 * @param debugLogger New debugLogger state.
	 */
	public static void setDebugLogger(boolean debugLogger)
	{
		RFHarvesterLogger.debugLogger = debugLogger;
	}

	/**
	 * @return InfoLog output stream
	 */
	public static OutputStream getInfoLog()
	{
		return infoLog;
	}

	/**
	 * @param infoLog New infoLog output stream
	 */
	public static void setInfoLog(OutputStream infoLog)
	{
		RFHarvesterLogger.infoLog = infoLog;
	}

	/**
	 * @return warningLog output stream
	 */
	public static OutputStream getWarningLog()
	{
		return warningLog;
	}

	/**
	 * @param warningLog New errorLog output stream
	 */
	public static void setWarningLog(OutputStream warningLog)
	{
		RFHarvesterLogger.warningLog = warningLog;
	}

	/**
	 * @return errorLog output stream
	 */
	public static OutputStream getErrorLog()
	{
		return errorLog;
	}

	/**
	 * @param errorLog New errorLog output stream
	 */
	public static void setErrorLog(OutputStream errorLog)
	{
		RFHarvesterLogger.errorLog = errorLog;
	}

	/**
	 * @return debugLog output stream
	 */
	public static OutputStream getDebugLog()
	{
		return debugLog;
	}

	/**
	 * @param debugLog New debugLog output stream
	 */
	public static void setDebugLog(OutputStream debugLog)
	{
		RFHarvesterLogger.debugLog = debugLog;
	}

	/**
	 * @return errorLog output stream
	 */
	public static String exceptionToString(Exception e)
	{
		StringWriter exceptionStringWriter = new StringWriter();
		PrintWriter exceptionPrintWriter = new PrintWriter(exceptionStringWriter);
		e.printStackTrace(exceptionPrintWriter);
		exceptionPrintWriter.flush();

		String exceptionString = exceptionStringWriter.toString();

		if(exceptionString.contains("\n"))
		{
			String result = "";
			for(String line : exceptionString.split("\n"))
			{
				if(line!=null)
				{
					result+=("\n                                                 " + line.trim());
				}
			}
			return result;
		}
		else
		{
			return exceptionString;
		}
	}

	private static void initTable()
	{
		String query;
		Statement uploadDBStatement = null;
		try
		{
			int type = ResultSet.TYPE_FORWARD_ONLY;
			int mode = ResultSet.CONCUR_READ_ONLY;
			uploadDBStatement = uploadDBConnection.createStatement(type, mode);

			query = "DROP TABLE IF EXISTS " + tableName;
			RFHarvesterLogger.debug(query);
			uploadDBStatement.execute(query);
			
			//Create the filled table
			query = "CREATE TABLE " + tableName + "(id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY, date TIMESTAMP, thread INTEGER UNSIGNED, type VARCHAR(9), message TEXT)";
			System.out.println(query);
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

	static
	{
		System.out.println("Initializing " + RFHarvesterLogger.class.getName());
		infoLog = new NullOutputStream();
		warningLog = new NullOutputStream();
		errorLog = new NullOutputStream();
		debugLog = new NullOutputStream();
		

		String jdbc = "Fatal error NOJDBC";
		String curDB = "Fatal error NODBNAME";
		try
		{
			jdbc = "com.mysql.jdbc.Driver";
			System.out.println("Init " + jdbc);
			Class.forName(jdbc);
			curDB = RFHarvesterLogger.uploadDB;
			RFHarvesterLogger.uploadDBConnection = DriverManager.getConnection(uploadDB, uploadDBlogin, uploadDBpassword);
			System.out.println("Connection with " + uploadDB + " established");
			RFHarvesterLogger.initTable();
		}
		catch(ClassNotFoundException e)
		{
			System.out.println(jdbc + "not found " + "\n" + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with missing library.
		}
		catch(SQLException e)
		{
			System.out.println("Unable to establish connection with" + curDB + "\n" + e.toString());
			e.printStackTrace();
			System.exit(0); // Program won't run with a missing library.
		}
	}

	/**
	 * @see info.
	 * @see error.
	 * @see debug.
	 * @param S String to write into out.
	 * @param out OutpuStream.
	 */
	private static void write(String typeS, String S, OutputStream out)
	{
		try
		{
			long dateTime = System.currentTimeMillis();
			long threadID = Thread.currentThread().getId();
			out.write((((datation) ? RFHarvesterDatation.getDateHour(dateTime) + " : " : "") + "[T" + ((threadID < 100) ? "0" : "") + ((threadID < 10) ? "0" : "") + threadID + "] : " + typeS + " : " + S + "\n").getBytes());
			out.flush();
			String query = "";
			Statement uploadDBStatement = null;
			try
			{
				int type = ResultSet.TYPE_FORWARD_ONLY;
				int mode = ResultSet.CONCUR_READ_ONLY;
				uploadDBStatement = uploadDBConnection.createStatement(type, mode);
				query = "INSERT INTO " + tableName + "(date, thread, type, message) VALUES(FROM_UNIXTIME("+dateTime+"),"+threadID+",\""+typeS.trim()+"\",\""+S.replaceAll("\"", "\"\"")+"\")";
				uploadDBStatement.execute(query);
			}
			catch(SQLException e)
			{
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
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Write S into info-log-file and all-log-File.
	 * 
	 * @param S text to write.
	 */
	public static void info(String S)
	{
		if(infoLogger)
		{
			write("[INFO]   ", S, infoLog);
		}
	}

	/**
	 * Write S into error-log-file and all-log-File.
	 * 
	 * @param S text to write.
	 */
	public static void warning(String S)
	{
		if(warningLogger)
		{
			write("[WARNING]", S, warningLog);
		}
	}

	/**
	 * Write S into error-log-file and all-log-File.
	 * 
	 * @param S text to write.
	 */
	public static void error(String S)
	{
		if(errorLogger)
		{
			write("[ERROR]  ", S, errorLog);
		}
	}

	/**
	 * Write S into debug-log-file and all-log-File.
	 * 
	 * @param S text to write.
	 */
	public static void debug(String S)
	{
		if(debugLogger)
		{
			write("[DEBUG]  ", S, debugLog);
		}
	}

	protected void finalize() throws Throwable
	{
		try
		{
			infoLog.close();
			warningLog.close();
			errorLog.close();
			debugLog.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			super.finalize();
		}
	}
}

// SEXY :D
//                          8888  8888888
//                   888888888888888888888888
//                8888:::8888888888888888888888888
//              8888::::::8888888888888888888888888888
//             88::::::::888:::8888888888888888888888888
//           88888888::::8:::::::::::88888888888888888888
//         888 8::888888::::::::::::::::::88888888888   888
//            88::::88888888::::m::::::::::88888888888    8
//          888888888888888888:M:::::::::::8888888888888
//         88888888888888888888::::::::::::M88888888888888
//         8888888888888888888888:::::::::M8888888888888888
//          8888888888888888888888:::::::M888888888888888888
//         8888888888888888::88888::::::M88888888888888888888
//       88888888888888888:::88888:::::M888888888888888   8888
//      88888888888888888:::88888::::M::;o*M*o;888888888    88
//     88888888888888888:::8888:::::M:::::::::::88888888    8
//    88888888888888888::::88::::::M:;:::::::::::888888888
//   8888888888888888888:::8::::::M::aAa::::::::M8888888888       8
//   88   8888888888::88::::8::::M:::::::::::::888888888888888 8888
//  88  88888888888:::8:::::::::M::::::::::;::88:88888888888888888
//  8  8888888888888:::::::::::M::"@@@@@@@"::::8w8888888888888888
//   88888888888:888::::::::::M:::::"@a@":::::M8i888888888888888
//  8888888888::::88:::::::::M88:::::::::::::M88z88888888888888888
// 8888888888:::::8:::::::::M88888:::::::::MM888!888888888888888888
// 888888888:::::8:::::::::M8888888MAmmmAMVMM888*88888888   88888888
// 888888 M:::::::::::::::M888888888:::::::MM88888888888888   8888888
// 8888   M::::::::::::::M88888888888::::::MM888888888888888    88888
//  888   M:::::::::::::M8888888888888M:::::mM888888888888888    8888
//   888  M::::::::::::M8888:888888888888::::m::Mm88888 888888   8888
//    88  M::::::::::::8888:88888888888888888::::::Mm8   88888   888
//    88  M::::::::::8888M::88888::888888888888:::::::Mm88888    88
//    8   MM::::::::8888M:::8888:::::888888888888::::::::Mm8     4
//        8M:::::::8888M:::::888:::::::88:::8888888::::::::Mm    2
//       88MM:::::8888M:::::::88::::::::8:::::888888:::M:::::M
//      8888M:::::888MM::::::::8:::::::::::M::::8888::::M::::M
//     88888M:::::88:M::::::::::8:::::::::::M:::8888::::::M::M
//    88 888MM:::888:M:::::::::::::::::::::::M:8888:::::::::M:
//    8 88888M:::88::M:::::::::::::::::::::::MM:88::::::::::::M
//      88888M:::88::M::::::::::*88*::::::::::M:88::::::::::::::M
//     888888M:::88::M:::::::::88@@88:::::::::M::88::::::::::::::M
//     888888MM::88::MM::::::::88@@88:::::::::M:::8::::::::::::::*8
//     88888  M:::8::MM:::::::::*88*::::::::::M:::::::::::::::::88@@
//     8888   MM::::::MM:::::::::::::::::::::MM:::::::::::::::::88@@
//      888    M:::::::MM:::::::::::::::::::MM::M::::::::::::::::*8
//      888    MM:::::::MMM::::::::::::::::MM:::MM:::::::::::::::M
//       88     M::::::::MMMM:::::::::::MMMM:::::MM::::::::::::MM
//        88    MM:::::::::MMMMMMMMMMMMMMM::::::::MMM::::::::MMM
//         88    MM::::::::::::MMMMMMM::::::::::::::MMMMMMMMMM
//          88   8MM::::::::::::::::::::::::::::::::::MMMMMM
//           8   88MM::::::::::::::::::::::M:::M::::::::MM
//               888MM::::::::::::::::::MM::::::MM::::::MM
//              88888MM:::::::::::::::MMM:::::::mM:::::MM
//              888888MM:::::::::::::MMM:::::::::MMM:::M
//             88888888MM:::::::::::MMM:::::::::::MM:::M
//            88 8888888M:::::::::MMM::::::::::::::M:::M
//            8  888888 M:::::::MM:::::::::::::::::M:::M:
//               888888 M::::::M:::::::::::::::::::M:::MM
//              888888  M:::::M::::::::::::::::::::::::M:M
//              888888  M:::::M:::::::::@::::::::::::::M::M
//              88888   M::::::::::::::@@:::::::::::::::M::M
//             88888   M::::::::::::::@@@::::::::::::::::M::M
//            88888   M:::::::::::::::@@::::::::::::::::::M::M
//           88888   M:::::m::::::::::@::::::::::Mm:::::::M:::M
//           8888   M:::::M:::::::::::::::::::::::MM:::::::M:::M
//          8888   M:::::M:::::::::::::::::::::::MMM::::::::M:::M
//         888    M:::::Mm::::::::::::::::::::::MMM:::::::::M::::M
//       8888    MM::::Mm:::::::::::::::::::::MMMM:::::::::m::m:::M
//      888      M:::::M::::::::::::::::::::MMM::::::::::::M::mm:::M
//   8888       MM:::::::::::::::::::::::::MM:::::::::::::mM::MM:::M:
//              M:::::::::::::::::::::::::M:::::::::::::::mM::MM:::Mm
//             MM::::::m:::::::::::::::::::::::::::::::::::M::MM:::MM
//             M::::::::M:::::::::::::::::::::::::::::::::::M::M:::MM
//            MM:::::::::M:::::::::::::M:::::::::::::::::::::M:M:::MM
//            M:::::::::::M88:::::::::M:::::::::::::::::::::::MM::MMM
//            M::::::::::::8888888888M::::::::::::::::::::::::MM::MM
//            M:::::::::::::88888888M:::::::::::::::::::::::::M::MM
//            M::::::::::::::888888M:::::::::::::::::::::::::M::MM
//            M:::::::::::::::88888M:::::::::::::::::::::::::M:MM
//            M:::::::::::::::::88M::::::::::::::::::::::::::MMM
//            M:::::::::::::::::::M::::::::::::::::::::::::::MMM
//            MM:::::::::::::::::M::::::::::::::::::::::::::MMM
//             M:::::::::::::::::M::::::::::::::::::::::::::MMM
//             MM:::::::::::::::M::::::::::::::::::::::::::MMM
//              M:::::::::::::::M:::::::::::::::::::::::::MMM
//              MM:::::::::::::M:::::::::::::::::::::::::MMM
//               M:::::::::::::M::::::::::::::::::::::::MMM
//               MM:::::::::::M::::::::::::::::::::::::MMM
//                M:::::::::::M:::::::::::::::::::::::MMM
//                MM:::::::::M:::::::::::::::::::::::MMM
//                 M:::::::::M::::::::::::::::::::::MMM
//                 MM:::::::M::::::::::::::::::::::MMM
//                  MM::::::M:::::::::::::::::::::MMM
//                  MM:::::M:::::::::::::::::::::MMM
//                   MM::::M::::::::::::::::::::MMM
//                   MM:::M::::::::::::::::::::MMM
//                    MM::M:::::::::::::::::::MMM
//                    MM:M:::::::::::::::::::MMM
//                     MMM::::::::::::::::::MMM
//                     MM::::::::::::::::::MMM
//                      M:::::::::::::::::MMM
//                     MM::::::::::::::::MMM
//                     MM:::::::::::::::MMM
//                     MM::::M:::::::::MMM:
//                     mMM::::MM:::::::MMMM
//                      MMM:::::::::::MMM:M
//                      mMM:::M:::::::M:M:M
//                       MM::MMMM:::::::M:M
//                       MM::MMM::::::::M:M
//                       mMM::MM::::::::M:M
//                        MM::MM:::::::::M:M
//                        MM::MM::::::::::M:m
//                        MM:::M:::::::::::MM
//                        MMM:::::::::::::::M:
//                        MMM:::::::::::::::M:
//                        MMM::::::::::::::::M
//                        MMM::::::::::::::::M
//                        MMM::::::::::::::::Mm
//                         MM::::::::::::::::MM
//                         MMM:::::::::::::::MM
//                         MMM:::::::::::::::MM
//                         MMM:::::::::::::::MM
//                         MMM:::::::::::::::MM
//                          MM::::::::::::::MMM
//                          MMM:::::::::::::MM
//                          MMM:::::::::::::MM
//                          MMM::::::::::::MM
//                           MM::::::::::::MM
//                           MM::::::::::::MM
//                           MM:::::::::::MM
//                           MMM::::::::::MM
//                           MMM::::::::::MM
//                            MM:::::::::MM
//                            MMM::::::::MM
//                            MMM::::::::MM
//                             MM::::::::MM
//                             MMM::::::MM
//                             MMM::::::MM
//                              MM::::::MM
//                              MM::::::MM
//                               MM:::::MM
//                               MM:::::MM:
//                               MM:::::M:M
//                               MM:::::M:M
//                               :M::::::M:
//                              M:M:::::::M
//                             M:::M::::::M
//                            M::::M::::::M
//                           M:::::M:::::::M
//                          M::::::MM:::::::M
//                          M:::::::M::::::::M
//                          M;:;::::M:::::::::M
//                          M:m:;:::M::::::::::M
//                          MM:m:m::M::::::::;:M
//                           MM:m::MM:::::::;:;M
//                            MM::MMM::::::;:m:M
//                             MMMM MM::::m:m:MM
//                                   MM::::m:MM
//                                    MM::::MM
//                                     MM::MM
//                                      MMMM