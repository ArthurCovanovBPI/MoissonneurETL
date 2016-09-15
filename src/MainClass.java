import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.TeeOutputStream;

import bpi.replication.MySQLReplicator;
import bpi.replication.MySQLReplicatorException;

import rfharvester.ExitCodes;
import rfharvester.RFHarvesterConfigurationClassException;
import rfharvester.download.PortfolioDownloader;
import rfharvester.download.RFHarvesterDownloaderInterface;
import rfharvester.download.RFHarvesterDownloaderV2ClassException;
import rfharvester.logger.RFHarvesterDatation;
import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.upload.RFHarvesterUploaderInterface;
import rfharvester.upload.RFHarvesterUploaderV2ClassException;
import rfharvester.upload.UploadAuthoritiesSolr;
import rfharvester.upload.UploadAuthoritiesSolr1;
import rfharvester.upload.UploadCollectionsMySQL;
import rfharvester.upload.UploadControlsMySQL;
import rfharvester.upload.UploadMetadatasMySQL;
import rfharvester.upload.UploadNoticesSolr;
import rfharvester.upload.UploadNoticesSolr1;
import rfharvester.upload.UploadNoticesSolr5;
import rfharvester.upload.UploadNoticesSolrV2;
import rfharvester.upload.UploadVolumesMySQL;
import rfharvester.upload.UploadPortfolioDatasMySQL;

/*
 * BPI Harvester is separated in 3 parts:
 * 1 - Get data from source databases
 * 2 - Process data
 * 3 - Store them into target databases
 */

/**
 * BPI Harvester starting class
 * 
 * @author ArthurCovanov
 */
public class MainClass
{
	private void harvestPortfolio()
	{
		final int collectionId = 5;

		RFHarvesterState.updateConfiguration("Portfolio");
		if(RFHarvesterState.checkRunningStatus() != 1)
		{
			RFHarvesterState.updateStatus("ERREUR!!!");
			RFHarvesterState.updateMessage("Impossible de lancer une moissons tant que l'historique contient des moissons ayant le statut: 'En cours'");
			System.exit(ExitCodes.EX_NOPERM.value());
		}

		final ArrayList<RFHarvesterUploaderInterface> uploadsList = new ArrayList<RFHarvesterUploaderInterface>();
		final RFHarvesterDownloaderInterface DI = new PortfolioDownloader(collectionId);
//		long downloadMillis = 0;
//		try
//		{
//			Date downloadDate = RFHarvesterDatation.DataToDate(2015, 2, 1);
//			//downloadDate = RFHarvesterDatation.DataToDate(2015, 5, 4);
//			downloadMillis = downloadDate.getTime();
//		}
//		catch(ParseException e)
//		{
//			e.printStackTrace();
//			System.exit(0); // Program won't run with parsing error
//		}

//		try
//		{
//			//TODO: REMOVE THIS FUCKING SHIT!!!
//			// For fuck's sake!
//			// If I find guy who created this stupid replication between the harvested MySQL and the used MySQL I'll chop his fucking head off!!
//			MySQLReplicator replication113 = new MySQLReplicator("jdbc:mysql://10.1.2.113/", "root", "mysqlbpi");
//			MySQLReplicator replication114 = new MySQLReplicator("jdbc:mysql://10.1.2.114/", "root", "mysqlbpi");
//			if(replication113.replicationStatus()==false)
//			{
//				replication113.startReplication();
//				if(replication113.replicationStatus()==true)
//					RFHarvesterLogger.info("Replication started properly on 113");
//				else
//					throw new MySQLReplicatorException("Unable to start replication on 113");
//			}
//			else
//				RFHarvesterLogger.info("Replication already started on 113");
//			if(replication114.replicationStatus()==false)
//			{
//				replication114.startReplication();
//				if(replication114.replicationStatus()==true)
//					RFHarvesterLogger.info("Replication started properly on 114");
//				else
//					throw new MySQLReplicatorException("Unable to start replication on 114");
//			}
//			else
//				RFHarvesterLogger.info("Replication already started on 114");
//		}
//		catch(ClassNotFoundException | SQLException | InterruptedException | MySQLReplicatorException e)
//		{
//			e.printStackTrace();
//			RFHarvesterLogger.error(e.toString());
//			System.exit(0);
//		}

		final RFHarvesterUploaderInterface controlsMySQLUploader = new UploadControlsMySQL(collectionId);
		uploadsList.add(controlsMySQLUploader);
		final RFHarvesterUploaderInterface metadatasMySQLUploader = new UploadMetadatasMySQL(collectionId);
		uploadsList.add(metadatasMySQLUploader);
		final RFHarvesterUploaderInterface portfoliodatasMySQLUploader = new UploadPortfolioDatasMySQL(collectionId);
		uploadsList.add(portfoliodatasMySQLUploader);
		final RFHarvesterUploaderInterface volumesMySQLUploader = new UploadVolumesMySQL(collectionId);
		uploadsList.add(volumesMySQLUploader);
		final RFHarvesterUploaderInterface collectionsMySQLUploader = new UploadCollectionsMySQL(collectionId);
		uploadsList.add(collectionsMySQLUploader);

//		final RFHarvesterUploaderInterface noticesSOLRUploader = new UploadNoticesSolr(collectionId);
//		uploadsList.add(noticesSOLRUploader);
//		final RFHarvesterUploaderInterface noticesSOLR1Uploader = new UploadNoticesSolr1(collectionId);
//		uploadsList.add(noticesSOLR1Uploader);
		final RFHarvesterUploaderInterface noticesSOLR6Uploader = new UploadNoticesSolr5(collectionId);
		uploadsList.add(noticesSOLR6Uploader);

		RFHarvesterLogger.info("Begin initialisation phase");
		ExecutorService es = Executors.newCachedThreadPool();
		for(final RFHarvesterUploaderInterface UI : uploadsList)
			es.execute(new Runnable()
			{
				public void run()
				{
					UI.initTable();
				}
			});

		es.shutdown();
		try
		{
			while(!es.awaitTermination(10, TimeUnit.SECONDS));
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
			System.exit(0);
		}

		final long usedDownloadMillis = 0;
		RFHarvesterLogger.info("Begin backup/downloading phase");
		es = Executors.newCachedThreadPool();
//		for(final RFHarvesterUploaderInterface UI : uploadsList)
//			es.execute(new Runnable()
//			{
//				public void run()
//				{
//					UI.copyIntoOld();
//				}
//			});
		es.execute(new Runnable()
		{
			public void run()
			{
				try
				{
					DI.download(uploadsList, usedDownloadMillis);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(0);
				}
			}
		});

		es.shutdown();
		try
		{
			while(!es.awaitTermination(10, TimeUnit.SECONDS));
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
			System.exit(0);
		}

		RFHarvesterLogger.info("Begin merging phase");
		es = Executors.newCachedThreadPool();


		controlsMySQLUploader.mergeOldTable();
//		noticesSOLRUploader.mergeOldTable();
//		noticesSOLR1Uploader.mergeOldTable();
		noticesSOLR6Uploader.mergeOldTable();
		metadatasMySQLUploader.mergeOldTable();
		volumesMySQLUploader.mergeOldTable();
		portfoliodatasMySQLUploader.mergeOldTable();
		collectionsMySQLUploader.mergeOldTable();

		RFHarvesterLogger.info("Begin replacement phase");
		es = Executors.newCachedThreadPool();
//		es.execute(new Runnable()
//		{
//			public void run()
//			{
//				noticesSOLRUploader.replaceOldTable();
//			}
//		});
//		es.execute(new Runnable()
//		{
//			public void run()
//			{
//				noticesSOLR1Uploader.replaceOldTable();
//			}
//		});
		es.execute(new Runnable()
		{
			public void run()
			{
				noticesSOLR6Uploader.replaceOldTable();
			}
		});
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

		es = Executors.newCachedThreadPool();
		es.execute(new Runnable()
		{
			public void run()
			{
				controlsMySQLUploader.replaceOldTable();
			}
		});
		es.execute(new Runnable()
		{
			public void run()
			{
				metadatasMySQLUploader.replaceOldTable();
			}
		});
		es.execute(new Runnable()
		{
			public void run()
			{
				portfoliodatasMySQLUploader.replaceOldTable();
			}
		});
		es.execute(new Runnable()
		{
			public void run()
			{
				volumesMySQLUploader.replaceOldTable();
			}
		});
		es.execute(new Runnable()
		{
			public void run()
			{
				collectionsMySQLUploader.replaceOldTable();
			}
		});
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

	private void harvestAuthorities()
	{

		ArrayList<RFHarvesterUploaderInterface> uploadsList = new ArrayList<RFHarvesterUploaderInterface>();
		final RFHarvesterUploaderInterface authoritiesSOLRUploader = new UploadAuthoritiesSolr();
		final RFHarvesterUploaderInterface authoritiesSOLR1Uploader = new UploadAuthoritiesSolr1();
		uploadsList.add(authoritiesSOLRUploader);
		uploadsList.add(authoritiesSOLR1Uploader);

		RFHarvesterLogger.info("Begin merging phase");
		authoritiesSOLRUploader.mergeOldTable();
		authoritiesSOLR1Uploader.mergeOldTable();

		RFHarvesterLogger.info("Begin replacement phase");
		authoritiesSOLRUploader.replaceOldTable();
		authoritiesSOLR1Uploader.replaceOldTable();
	}

	private void runConfiguration(String configurationIDString) throws SQLException, ClassNotFoundException, RFHarvesterConfigurationClassException, RFHarvesterDownloaderV2ClassException, RFHarvesterUploaderV2ClassException
	{
		RFHarvesterLogger.info("Running configuration: "+configurationIDString);
		try
		{
			int configurationID = Integer.parseInt(configurationIDString);
			HarvestConfiguration configuration = new HarvestConfiguration();
			configuration.loadConfiguration(configurationID);
			configuration.run();
		}
		catch(java.lang.NumberFormatException e)
		{
			RFHarvesterLogger.error("Unahandled configuration format: " + configurationIDString);
			throw e;
		}
	}

	/**
	 * Starting point
	 * 
	 * @param args
	 */
	public static void main(String args[])
	{
		System.out.println("Program initialisation");

		// Initializing Logs
		final long startTime = System.currentTimeMillis();

		System.out.println("Initializing logs");

		String logDirectory = "./log";
		String allLogFileName = "all.log";
		String infoLogFileName = "info.log";
		String warningLogFileName = "warning.log";
		String errorLogFileName = "error.log";
		String debugLogFileName = "debug.log";
		String harvesterLogFileName = "log/harvester.log";

		File logdir = new File(logDirectory);
		logdir.mkdir();
		File allFile = new File(logDirectory + "/" + allLogFileName);
		File infoFile = new File(logDirectory + "/" + infoLogFileName);
		File warningFile = new File(logDirectory + "/" + warningLogFileName);
		File errorFile = new File(logDirectory + "/" + errorLogFileName);
		File debugFile = new File(logDirectory + "/" + debugLogFileName);
		File harvesterFile = new File(harvesterLogFileName);

		allFile.delete();
		infoFile.delete();
		warningFile.delete();
		errorFile.delete();
		debugFile.delete();

		//Set loggers outputs
		try
		{
			FileOutputStream allFileOutputStream = new FileOutputStream(allFile, true);
			FileOutputStream harvesterFileOutputStream = new FileOutputStream(harvesterFile, true);
			FileOutputStream infoFileOutputStream = new FileOutputStream(infoFile, true);
			FileOutputStream warningFileOutputStream = new FileOutputStream(warningFile, true);
			FileOutputStream errorFileOutputStream = new FileOutputStream(errorFile, true);
			FileOutputStream debugFileOutputStream = new FileOutputStream(debugFile, true);
			RFHarvesterLogger.setInfoLog(new TeeOutputStream(System.out, new TeeOutputStream(harvesterFileOutputStream, new TeeOutputStream(allFileOutputStream, infoFileOutputStream))));
			RFHarvesterLogger.setWarningLog(new TeeOutputStream(System.out, new TeeOutputStream(harvesterFileOutputStream, new TeeOutputStream(allFileOutputStream, warningFileOutputStream))));
			RFHarvesterLogger.setErrorLog(new TeeOutputStream(System.out, new TeeOutputStream(harvesterFileOutputStream, new TeeOutputStream(allFileOutputStream, errorFileOutputStream))));
			RFHarvesterLogger.setDebugLog(new TeeOutputStream(System.out, new TeeOutputStream(harvesterFileOutputStream, new TeeOutputStream(allFileOutputStream, debugFileOutputStream))));
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
			System.exit(ExitCodes.EX_CANTCREAT.value()); // Program won't run without correctly set loggers.
		}

		RFHarvesterLogger.setDatation(true);
		RFHarvesterLogger.info("Logs correctly initialized");

		RFHarvesterState.begin();

		Runtime.getRuntime().addShutdownHook
		(
			new Thread()
			{
				@Override
				public void run()
				{
					if(RFHarvesterState.checkRunningStatus()>0)
					{
						RFHarvesterState.updateStatus("Interrompue");
						RFHarvesterState.updateMessage("Moisson interrompue.");
					}
					RFHarvesterState.endStatus();
					long endTime = System.currentTimeMillis();
					RFHarvesterLogger.info("Program ended at:   " + RFHarvesterDatation.getDateHour(endTime));
					RFHarvesterLogger.info("Program Total duration : " + RFHarvesterDatation.duration(endTime - startTime));
					RFHarvesterLogger.info("------------------------------------------------------------------------------------------------------------------------");
					RFHarvesterLogger.debug("------------------------------------------------------------------------------------------------------------------------");
					RFHarvesterLogger.warning("------------------------------------------------------------------------------------------------------------------------");
					RFHarvesterLogger.error("------------------------------------------------------------------------------------------------------------------------");
				}
			}
		);

		RFHarvesterLogger.info("Program started at: " + RFHarvesterDatation.getDateHour(startTime));

		MainClass program = new MainClass();
		ArrayList<String> arguments = new ArrayList<String>(Arrays.asList(args));

		int flags					= 0b00000000000000000000000000000000;
		final int harvestFlag		= 0b00000000000000000000000000000001;
		final int harvestParameter	= 0b00000000000000000000000000000100;

		for(String arg : arguments)
		{
			RFHarvesterLogger.debug(arg);
			String[] argWithValues = arg.split("=");
			switch(argWithValues[0])
			{
				case "--harvest":
					flags |= harvestFlag;
					if(argWithValues.length > 1)
						flags |= harvestParameter;
				break;
				// NEW WAY TO RUN CONFIGURATION, OTHER ONES ARE DEPRECATED
				case "--configuration":
					int exitCode = ExitCodes.EX_OK.value();
					if(argWithValues.length<=1)
					{
						RFHarvesterLogger.error("ERROR: Missing configuration value.");
						RFHarvesterState.updateStatus("ERREUR!!!");
						RFHarvesterState.updateMessage("Missing configuration value.");
						exitCode = ExitCodes.EX_USAGE.value();
					}
					else
					{
						try
						{
							RFHarvesterState.updateConfiguration("ID: " + argWithValues[1]);
							program.runConfiguration(argWithValues[1]);
							RFHarvesterState.updateStatus("Terminée");
						}
						catch(Exception e)
						{
							RFHarvesterLogger.error("FATAL ERROR!!!" + RFHarvesterLogger.exceptionToString(e));
							RFHarvesterState.updateStatus("ERREUR!!!");
							RFHarvesterState.updateMessage(e.getMessage());
							exitCode = ExitCodes.EX_SOFTWARE.value();
						}
					}
					System.exit(exitCode);
				break;

				case "--info":
				case "--list-properties":
				case "--version":
				break;
				default:
					System.out.println("Unknown property: " + argWithValues[0]);
					System.exit(ExitCodes.EX_USAGE.value());
				break;
			}
		}
		if(((flags & harvestFlag) != harvestFlag))
		{
			System.out.println("Missing mandatory property --harvest");
			System.exit(ExitCodes.EX_USAGE.value());
		}
		else if((flags & harvestParameter) != harvestParameter)
		{
			System.out.println("Property --harvest have no defined value");
			System.exit(ExitCodes.EX_USAGE.value());
		}

		String harvest = null;
		for(String arg : arguments)
		{
			String[] argWithValues = arg.split("=");
			switch(argWithValues[0])
			{
				case "--list-properties":
					System.getProperties().list(System.out);
				break;
				case "--restore":
					if(argWithValues.length > 2)
					{
						System.out.println("Property --harvest wrongly defined: " + arg.substring(10));
						System.exit(ExitCodes.EX_USAGE.value());
					}
					harvest = argWithValues[1];
				break;
				case "--harvest":
					if(argWithValues.length > 2)
					{
						System.out.println("Property --harvest wrongly defined: " + arg.substring(10));
						System.exit(ExitCodes.EX_USAGE.value());
					}
					harvest = argWithValues[1];
				break;
				default:
				break;
			}
		}

		if(((flags & harvestFlag) == harvestFlag))
		{
			switch(harvest)
			{
				case "portfolio":
					program.harvestPortfolio();
					RFHarvesterState.updateStatus("Terminée");
				break;
				case "authorities":
					program.harvestAuthorities();
					RFHarvesterState.updateStatus("Terminée");
				break;
				default:
					RFHarvesterLogger.error("Unrecognized harvesting value: " + harvest);
				break;
			}
		}
	}
}

//	  MEOW!
//
//	  /\_/\
//	=( °w° )=
//	  )   (  //
//	 (__ __)//