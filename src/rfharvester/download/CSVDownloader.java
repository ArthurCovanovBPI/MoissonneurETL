package rfharvester.download;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import CSV.CSVFileReader;
import CSV.CSVFileReaderException;

import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.transformator.RFHarvesterTransformatorInterfaceV2;
import rfharvester.upload.RFHarvesterUploaderV2Exception;
import rfharvester.upload.RFHarvesterUploaderV2Interface;

public class CSVDownloader implements RFHarvesterDownloaderInterfaceV2
{
	private String filePath;
	private String CSVSeparator;
	private String defaultDocumentType;
	private RFHarvesterTransformatorInterfaceV2 transformator;
	private RFHarvesterUploaderV2Interface uploader;

	public CSVDownloader(String filePath, String CSVSeparator, RFHarvesterTransformatorInterfaceV2 transformator, RFHarvesterUploaderV2Interface uploader, String defaultDocumentType)
	{
		this.filePath = filePath;
		this.CSVSeparator = CSVSeparator;
		this.defaultDocumentType = defaultDocumentType;
		this.transformator = transformator;
		this.uploader = uploader;
	}

	@Override
	public void download() throws RFHarvesterDownloaderV2Exception
	{
		CSVFileReader csv;

		try
		{

			csv = new CSVFileReader(filePath, CSVSeparator);
		}
		catch(IOException e)
		{
			throw new RFHarvesterDownloaderV2Exception(e);
		}

		long s = System.currentTimeMillis();
		int nb = csv.getLinesCount();
		int inserts = 0;

		HashMap<String, ArrayList<String>> transformation = null;
		do
		{
			try
			{
				transformation = csv.nextLine();

				transformation = transformator.transform(transformation); //Have to contain OAI_ID
				if(defaultDocumentType != null)
				{
					ArrayList<String> OAIDefaultDocumentType = new ArrayList<String>();
					OAIDefaultDocumentType.add(defaultDocumentType);
					transformation.put("OAI_defaultDocumentType", OAIDefaultDocumentType);
				}

				try
				{
					uploader.insertRow(transformation);
				}
				catch (RFHarvesterUploaderV2Exception e)
				{
					RFHarvesterLogger.warning("Unable to insert line " + csv.getLine() + "(" + transformation + ")" + RFHarvesterLogger.exceptionToString(e));
				}
				inserts++;

				if(csv.getLine() % 100 == 0)
					RFHarvesterLogger.info(csv.getLine() + "/" + nb + " lines parsed");
			}
			catch (CSVFileReaderException e)
			{
				RFHarvesterLogger.warning("Unable to parse CSV line " + csv.getLine() + RFHarvesterLogger.exceptionToString(e));
			}

		}while(transformation != null);

		RFHarvesterState.updateHarvestedDocuments(inserts);


		long e = System.currentTimeMillis();
		RFHarvesterLogger.info(nb + " records parsed");
		RFHarvesterLogger.info("Total duration: " + ((e - s) / 1000) + " secs");
	}
}
