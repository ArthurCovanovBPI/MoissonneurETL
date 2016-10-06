package rfharvester.download;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import CSV.CSVReaderException;
import CSV.CSVURLReader;

import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.transformator.RFHarvesterTransformatorInterfaceV2;
import rfharvester.upload.RFHarvesterUploaderV2Exception;
import rfharvester.upload.RFHarvesterUploaderV2Interface;

public class CSVURLDownloader implements RFHarvesterDownloaderInterfaceV2
{
	private String URL;
	private String CSVSeparator;
	private String defaultDocumentType;
	private RFHarvesterTransformatorInterfaceV2 transformator;
	private RFHarvesterUploaderV2Interface uploader;

	public CSVURLDownloader(String URL, String CSVSeparator, RFHarvesterTransformatorInterfaceV2 transformator, RFHarvesterUploaderV2Interface uploader, String defaultDocumentType)
	{
		this.URL = URL;
		this.CSVSeparator = CSVSeparator;
		this.defaultDocumentType = defaultDocumentType;
		this.transformator = transformator;
		this.uploader = uploader;
	}

	@Override
	public void download() throws RFHarvesterDownloaderV2Exception
	{
		CSVURLReader csv;

		try
		{
			csv = new CSVURLReader(URL, CSVSeparator);
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

//				System.out.println("#" + transformation);
				transformation = transformator.transform(transformation); //Have to contain OAI_ID
				if(defaultDocumentType != null)
				{
					ArrayList<String> OAIDefaultDocumentType = new ArrayList<String>();
					OAIDefaultDocumentType.add(defaultDocumentType);
					transformation.put("OAI_defaultDocumentType", OAIDefaultDocumentType);
				}
//				System.out.println("~" + transformation);

				try
				{
					uploader.insertRow(transformation);
				}
				catch (RFHarvesterUploaderV2Exception e)
				{
					RFHarvesterLogger.warning("Unable to insert line " + csv.getLine() + "(" + transformation + ")" + RFHarvesterLogger.exceptionToString(e));
				}
				inserts++;

				if(csv.getLine() % 1000 == 0)
					RFHarvesterLogger.info(csv.getLine() + "/" + nb + " lines parsed");
			}
			catch (CSVReaderException e)
			{
				RFHarvesterLogger.warning("Unable to parse CSV line " + csv.getLine() + RFHarvesterLogger.exceptionToString(e));
//				continue;
			}

		}while(csv.getLine() < nb);

		RFHarvesterState.updateHarvestedDocuments(inserts);


		long e = System.currentTimeMillis();
		RFHarvesterLogger.info(nb + " records parsed");
		RFHarvesterLogger.info("Total duration: " + ((e - s) / 1000) + " secs");
	}
}
