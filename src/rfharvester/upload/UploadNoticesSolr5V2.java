package rfharvester.upload;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import rfharvester.logger.RFHarvesterLogger;
import rfharvester.utils.RFHarvesterUtilities;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class UploadNoticesSolr5V2 implements RFHarvesterUploaderV2Interface
{
	private final String zookeeper1 = "10.1.2.105:2181";
	private final String zookeeper2 = "10.1.2.106:2181";
	private final String zookeeper3 = "10.1.2.107:2181";
	private final List<String> zookeepers = Arrays.asList(zookeeper1, zookeeper2, zookeeper3);
	CloudSolrClient client;

	private SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private Collection<SolrInputDocument> notices = new ArrayList<SolrInputDocument>();

	private final int collectionID;
	private final String collectionName;
	private final HashMap<String, String> disponibilite;
	private int recomandedCommit = 500;

	private String todayDate;

	SolrInputDocument lastDocument = null;

	private void initTodayDate()
	{
		Calendar c = new GregorianCalendar();
		c.set(Calendar.HOUR_OF_DAY, 0); //anything 0 - 23
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		Date today = c.getTime();

		todayDate = DF.format(today).toString();
	}

	public void dropLast()
	{
		if(lastDocument==null)
			return;
		notices.remove(lastDocument);
		RFHarvesterLogger.debug("Dropped last Solr document.");
		lastDocument = null;
	}

	private void initTable() throws RFHarvesterUploaderV2Exception
	{
		try
		{
			RFHarvesterLogger.debug("client.deleteByQuery(\"collection_id:(" + collectionID + ")\");");
			UpdateResponse SOLRresponse = client.deleteByQuery("collection_id:(" + collectionID + ")");
			RFHarvesterLogger.debug(SOLRresponse.toString());
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			try
			{
				client.rollback();
			}
			catch (SolrServerException | IOException e1)
			{
				e1.printStackTrace();
			}
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
	}

	public UploadNoticesSolr5V2(int recomandedCommit, int collectionID, String collectionName, HashMap<String, String> disponibilite) throws RFHarvesterUploaderV2Exception
	{
		RFHarvesterLogger.info("Initializing " + this.getClass().getName());
		String zookeepersList = zookeepers.toString().substring(1, zookeepers.toString().length()-1).replaceAll(" ", "");

		client = new CloudSolrClient(zookeepersList);
		System.out.println("Connection with " + client.getZkHost() + " established");
		client.setDefaultCollection("notices");

		this.collectionID = collectionID;
		this.collectionName = collectionName;
		this.disponibilite = disponibilite;
		this.recomandedCommit=recomandedCommit;
		
		initTable();
		initTodayDate();
	}

	public void commit()
	{
		UpdateResponse responseAdding = null;
		try
		{
			if(notices.size()<=0)
			{
				RFHarvesterLogger.warning("Empty SOLR notices set");
				return;
			}
			responseAdding = client.add(notices);
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
		notices.clear();
		RFHarvesterLogger.debug("SOLR commit finished with status " + responseAdding.getStatus());// + " and last " + responseAdding.getQTime() + " milliseconds");
	}

	private Date simpleDateParse(String date) throws ParseException
	{
		if(date==null || date.isEmpty())
			return null;
		SimpleDateFormat ymd = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat ym  = new SimpleDateFormat("yyyyMM");
		SimpleDateFormat y   = new SimpleDateFormat("yyyy");
		Date result=null;
		try
		{
			result = ymd.parse(date);
		}
		catch(ParseException eydm)
		{
			try
			{
				result = ym.parse(date);
			}
			catch(ParseException eym)
			{
				result = y.parse(date);
			}
		}
		return result;
	}

	private float dateBoost(String date)
	{
		if(date==null)
			return 0.000001f;
		SimpleDateFormat ymd = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat ym  = new SimpleDateFormat("yyyyMM");
		SimpleDateFormat y   = new SimpleDateFormat("yyyy");
		Date result=null;
		try
		{
			result = ymd.parse(date);
		}
		catch(ParseException eydm)
		{
			try
			{
				result = ym.parse(date);
			}
			catch(ParseException eym)
			{
				try
				{
					result = y.parse(date);
				}
				catch(ParseException e)
				{
					RFHarvesterLogger.warning("Unable to parse in date format: " + date);
					e.printStackTrace();
				}
			}
		}
		if(result==null)
			return 0.000001f;

		SimpleDateFormat YF = new SimpleDateFormat("yyyy");
		String dY = YF.format(result).toString();
		float dateYear = (float)Integer.parseInt(dY);


		Calendar c = new GregorianCalendar();
		Date today = c.getTime();
		String cY = YF.format(today).toString();
		float curYear = (float)Integer.parseInt(cY);

		float delta = dateYear - curYear;
		if(delta<0)
			return 0.000001f;
		return 1000/delta;
	}

	private String normalizeTitleSort(final String in)
	{
		String out = in.replaceAll("[^a-z^A-Z^à^á^â^ã^ä^å^ò^ó^ô^õ^ö^ø^è^é^ê^ë^ç^ì^í^î^ï^ù^ú^û^ü^ÿ^ñ^_^0-9^\\)\\(^\\]^\\[^\\\"^;^[:punct:]]$", "").trim();
		return out;
	}

	@Override
	public String insertRow(HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2Exception
	{
		String out = null;

		SolrInputDocument document = new SolrInputDocument();

		document.addField("id", row.get("OAI_ID").get(0)+";"+collectionID);
		document.addField("collection_id", collectionID);
		document.addField("collection_name", collectionName);
		if(row.containsKey("controlID") && row.get("controlID")!=null)
			document.addField("controls_id", row.get("controlID").get(0));
		else
			throw new RFHarvesterUploaderV2Exception("Missing collectionID in "+ row.toString());

		ArrayList<String> titles = new ArrayList<String>();
		if(row.containsKey("titres"))
			titles.addAll(row.get("titres"));
		else
			throw new RFHarvesterUploaderV2Exception("Missing key titres");
		if(row.containsKey("relations"))
			titles.addAll(row.get("relations"));
		String titleString = RFHarvesterUtilities.arrayListToString(titles, "; ");
		if(titleString == null)
			throw new RFHarvesterUploaderV2Exception("Missing titles in " + row);
		document.addField("title", titleString);
		document.addField("title_sort", normalizeTitleSort(titleString));

		if(row.containsKey("sujets"))
			document.addField("subject", RFHarvesterUtilities.arrayListToString(row.get("sujets"), "; "));

		ArrayList<String> creators = new ArrayList<String>();

		if(row.containsKey("auteurs"))
			creators.addAll(row.get("auteurs"));
		if(row.containsKey("contributeurs"))
			creators.addAll(row.get("contributeurs"));

		document.addField("creator", RFHarvesterUtilities.arrayListToString(creators, "; "));

		ArrayList<String> keywords = RFHarvesterUtilities.createSet(row.get("titres"), row.get("auteurs"), row.get("sujets"), row.get("descriptions"), row.get("editeurs"), row.get("contributeurs"), row.get("coverages"), row.get("relations"), row.get("types"), row.get("relations"));
		String keywordsString = RFHarvesterUtilities.arrayListToString(keywords, "; ");
		document.addField("keyword", keywordsString);
		document.addField("autocomplete", keywordsString);

		if(row.containsKey("OAI_defaultDocumentType"))
		{
			document.addField("document_type", row.get("OAI_defaultDocumentType").get(0));
		}
		else if(row.containsKey("types"))
		{
			ArrayList<String> types = row.get("types");
			document.addField("document_type", RFHarvesterUtilities.arrayListToString(types, ", "));
		}

		document.addField("harvesting_date", todayDate);

		if(disponibilite.get("dispo_sur_poste")!=null)
			document.addField("dispo_sur_poste", disponibilite.get("dispo_sur_poste"));
		if(disponibilite.get("dispo_bibliotheque")!=null)
			document.addField("dispo_bibliotheque", disponibilite.get("dispo_bibliotheque"));
		if(disponibilite.get("dispo_access_libre")!=null)
			document.addField("dispo_access_libre", disponibilite.get("dispo_access_libre"));
		if(disponibilite.get("dispo_avec_reservation")!=null)
			document.addField("dispo_avec_reservation", disponibilite.get("dispo_avec_reservation"));
		if(disponibilite.get("dispo_avec_access_autorise")!=null)
			document.addField("dispo_avec_access_autorise", disponibilite.get("dispo_avec_access_autorise"));
		if(disponibilite.get("dispo_broadcast_group")!=null)
			document.addField("dispo_broadcast_group", disponibilite.get("dispo_broadcast_group").split(","));

		if(row.containsKey("langues"))
			document.addField("lang_exact", RFHarvesterUtilities.arrayListToString(row.get("langue"), "; "));

		if(row.containsKey("dates"))
		{
			String dateDocument = null;
			try
			{
				Date date = simpleDateParse(row.get("dates").get(0));
				dateDocument = DF.format(date).toString();
			}
			catch (ParseException e)
			{
				RFHarvesterLogger.warning("Unable to parse in date format: " + row.get("dates").get(0) + " for " + row.get("OAI_ID").get(0) + " - " + row.get("titres") + RFHarvesterLogger.exceptionToString(e));
			}
			if(dateDocument != null)
				document.addField("date_document", dateDocument);
//			if(collectionID == 5)
//				document.setDocumentBoost(dateBoost(dateDocument) + Integer.parseInt(row.get("solr_boost").get(0)));
		}
		else if(collectionID == 5)
			document.setDocumentBoost(Integer.parseInt(row.get("solr_boost").get(0)));

		lastDocument = document;

		notices.add(document);

		if(notices.size() >= recomandedCommit)
		{
			commit();
		}
		return out;
	}

	HashSet<String> errors = new HashSet<String>();

	public void storeErrorIdentifier(String dc_identifier)
	{
		errors.add(dc_identifier + ";" + collectionID);
	}

	public void cleanErrors() throws RFHarvesterUploaderV2Exception
	{
		for(String error : errors)
		{
			try
			{
				RFHarvesterLogger.info("client_new.deleteByQuery(\"id:(" + error + ")\");");
				UpdateResponse SOLRresponse = client.deleteByQuery("id:(" + error + ")");
				RFHarvesterLogger.debug(SOLRresponse.toString());
			}
			catch(SolrServerException | IOException e)
			{
				throw new RFHarvesterUploaderV2Exception(e); // Program won't run with uninitialized SOLR.
			}
		}
	}

	public void replaceOldTable() throws RFHarvesterUploaderV2Exception
	{
		try
		{
			RFHarvesterLogger.info("Commit Solr modifications");

			client.commit();
			RFHarvesterLogger.info("SOLR commited");
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			try
			{
				client.rollback();
				RFHarvesterLogger.info("Client rollbacked");
			}
			catch (SolrServerException | IOException e1)
			{
				e1.printStackTrace();
			}
		}
	}

	@Override
	public void end() throws RFHarvesterUploaderV2Exception
	{
		commit();
		replaceOldTable();
	}
}
