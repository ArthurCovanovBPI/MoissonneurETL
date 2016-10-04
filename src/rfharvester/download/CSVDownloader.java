package rfharvester.download;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.transformator.RFHarvesterTransformatorInterfaceV2;
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
		String link = filePath;
		RFHarvesterLogger.debug(link);
		URL url;

		int ptr = 0;
		StringBuffer buffer = null;
		URLConnection con = null;
		InputStreamReader IS = null;
		String xml = null;
		try
		{
			url = new URL(link);
			buffer = new StringBuffer();

			IS = new InputStreamReader(con.getInputStream(), "UTF-8");

			ptr = 0;
			buffer = new StringBuffer();
			while((ptr = IS.read()) != -1)
				buffer.append((char) ptr);

			RFHarvesterLogger.info("Connection  with " + link + " openned");
			xml = buffer.toString();
		}
		catch(IOException e)
		{
			throw new RFHarvesterDownloaderV2Exception(e);
		}

		OAIPage page = new OAIPage(xml);

		String resumptionToken = page.getResuptionToken();
		String prevResumptionToken = "";

		long start = System.currentTimeMillis();
		long startm = start;
		long s = start;
		int count = 0;
		int nb = page.getRecords().size();
		int inserts = 0;

		for(OAIRecord record : page.getRecords())
		{
			try
			{
				HashMap<String, ArrayList<String>> transformation = transformator.transform(record.getMetadata().getValues());

				ArrayList<String> ID = new ArrayList<String>();
				ID.add(record.getHeader().getIdentifier().replaceAll("·", "."));
				transformation.put("OAI_ID", ID);
				if(defaultDocumentType != null)
				{
					ArrayList<String> OAIDefaultDocumentType = new ArrayList<String>();
					OAIDefaultDocumentType.add(defaultDocumentType);
					transformation.put("OAI_defaultDocumentType", OAIDefaultDocumentType);
				}
				uploader.insertRow(transformation);
				inserts++;
			}
			catch(Exception e)
			{
				RFHarvesterLogger.error("Unable to insert record: " + record.getXML() + RFHarvesterLogger.exceptionToString(e));
				e.printStackTrace();
			}
		}

		RFHarvesterState.updateHarvestedDocuments(inserts);

		if(page.getCursor()==null)
			RFHarvesterLogger.info(count + page.getRecords().size() + "/" + page.getCompleteListSize() + " - previousTokens/nextToken : " + prevResumptionToken + " / " + resumptionToken);
		else
			RFHarvesterLogger.info(Integer.parseInt(page.getCursor()) + page.getRecords().size() + "/" + page.getCompleteListSize() + " - previousTokens/nextToken : " + prevResumptionToken + " / " + resumptionToken);

		long e = System.currentTimeMillis();
		RFHarvesterLogger.info(nb + " records parsed");
		RFHarvesterLogger.info("Total duration: " + ((e - s) / 1000) + " secs");
	}
}
