package rfharvester.download;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;

import OAI.OAIPage;
import OAI.OAIRecord;

import rfharvester.logger.RFHarvesterLogger;
import rfharvester.logger.RFHarvesterState;
import rfharvester.transformator.RFHarvesterTransformatorInterfaceV2;
import rfharvester.upload.RFHarvesterUploaderV2Interface;

public class OAIDownloader implements RFHarvesterDownloaderInterfaceV2
{
	private String URL;
	private String URLADDITION;
	private String defaultDocumentType;
	private RFHarvesterTransformatorInterfaceV2 transformator;
	private RFHarvesterUploaderV2Interface uploader;

	private Proxy proxy;

	public OAIDownloader(String URL, String URLADDITION, RFHarvesterTransformatorInterfaceV2 transformator, RFHarvesterUploaderV2Interface uploader, String defaultDocumentType)
	{
		this.URL = URL;
		this.URLADDITION = ((URLADDITION==null)? "" : URLADDITION);
		this.defaultDocumentType = defaultDocumentType;
		this.transformator = transformator;
		this.uploader = uploader;
		this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("10.1.2.30", 3128));
	}

	@Override
	public void download() throws RFHarvesterDownloaderV2Exception
	{
		String link = URL + "?verb=ListRecords&metadataPrefix=oai_dc"+URLADDITION;
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
			
			con = url.openConnection(proxy);
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

		int nullXML = 0;

		while(resumptionToken != null && !resumptionToken.isEmpty())
		{
			try
			{
				url = new URL(URL + "?verb=ListRecords&resumptionToken=" + resumptionToken);
				con = url.openConnection(proxy);
				IS = new InputStreamReader(con.getInputStream(), "UTF-8");

				ptr = 0;
				buffer = new StringBuffer();
				while((ptr = IS.read()) != -1)
					buffer.append((char) ptr);
				xml = buffer.toString();
			}
			catch(Exception e)
			{
				nullXML++;
				if(nullXML>10)
				{
					RFHarvesterLogger.error("More thant 10 attempts for resumptionToken : " + resumptionToken);
					long endTime = System.currentTimeMillis();
					RFHarvesterLogger.info("Total duration: " + ((endTime - s) / 1000) + " secs : " + prevResumptionToken + " - " + resumptionToken);
					RFHarvesterLogger.info(nb + " records parsed");
					throw new RFHarvesterDownloaderV2Exception(e);
				}
				RFHarvesterLogger.warning(nullXML + " : NULL XML FOR TOKEN " + resumptionToken);
				continue;
			}

			if(xml == null || xml.isEmpty())
			{
				nullXML++;
				if(nullXML>10)
				{
					throw new RFHarvesterDownloaderV2Exception("More thant 10 attempts for resumptionToken : " + resumptionToken);
				}
				RFHarvesterLogger.warning(nullXML + " : NULL XML FOR TOKEN " + resumptionToken + "!!!");
				continue;
			}

			try
			{
				page = new OAIPage(xml);
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
			}
			catch(Exception e)
			{
				nullXML++;
				if(nullXML>10)
				{
					RFHarvesterLogger.error("More thant 10 attempts for resumptionToken : " + resumptionToken);
					long endTime = System.currentTimeMillis();
					RFHarvesterLogger.debug(xml);
					RFHarvesterLogger.info("Total duration: " + ((endTime - s) / 1000) + " secs : " + prevResumptionToken + " - " + resumptionToken);
					RFHarvesterLogger.info(nb + " records parsed");
					throw e;
				}
				RFHarvesterLogger.warning(nullXML + " : NULL PAGE FOR TOKEN " + resumptionToken + "!!!");
				e.printStackTrace();
				continue;
			}
			nullXML = 0;

			RFHarvesterState.updateHarvestedDocuments(inserts);

			prevResumptionToken = resumptionToken;
			resumptionToken = page.getResuptionToken();
			if(resumptionToken == null)
				break;

			if(page.getCursor()==null)
				RFHarvesterLogger.info(count + page.getRecords().size() + "/" + page.getCompleteListSize() + " - previousTokens/nextToken : " + prevResumptionToken + " / " + resumptionToken);
			else
				RFHarvesterLogger.info((Integer.parseInt(page.getCursor()) + page.getRecords().size()) + "/" + page.getCompleteListSize() + " - previousTokens/nextToken : " + prevResumptionToken + " / " + resumptionToken);

			nb += page.getRecords().size();
			count++;
			if(count % 400 == 0)
			{
				long elapsedTimeMillis = System.currentTimeMillis() - startm;
				long elapseMin = (elapsedTimeMillis / (60 * 1000));
				long elapseSec = elapsedTimeMillis - (elapseMin * 60 * 1000);
				elapseSec = elapseSec / 1000;
				elapsedTimeMillis = System.currentTimeMillis() - start;
				startm = start = System.currentTimeMillis();
			}
			else if(count % 100 == 0)
			{
				start = System.currentTimeMillis();
			}
		}
		long e = System.currentTimeMillis();
		RFHarvesterLogger.info(nb + " records parsed");
		RFHarvesterLogger.info("Total duration: " + ((e - s) / 1000) + " secs");
	}
}
