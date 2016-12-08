package rfharvester.indexor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class IndexorSolr5 implements IndexorInterface
{
	private final String downloadDB;
	private final String downloadDBlogin = "harvester";
	private final String downloadDBpassword = "harvester";

	private Connection downloadDBConnection = null;

	private final String zookeeper1 = "10.1.2.105:2181";
	private final String zookeeper2 = "10.1.2.106:2181";
	private final String zookeeper3 = "10.1.2.107:2181";
	private final List<String> zookeepers = Arrays.asList(zookeeper1, zookeeper2, zookeeper3);
	CloudSolrClient client;

	private SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private Collection<SolrInputDocument> notices = new ArrayList<SolrInputDocument>();

	private final int collectionID;
	private final String collectionName;
	private final String defaultDocumentType;
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

	private void initTable() throws IndexorException
	{
		try
		{
			RFHarvesterLogger.debug("client.deleteByQuery(\"collection_id:(" + collectionID + ")\");");
			UpdateResponse SOLRresponse = client.deleteByQuery("collection_id:(" + collectionID + ")");
			RFHarvesterLogger.debug(SOLRresponse.toString());
		}
		catch(SolrServerException | IOException e)
		{
			RFHarvesterLogger.error(RFHarvesterLogger.exceptionToString(e));
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
	}

	public IndexorSolr5(String downloadDB, int recomandedCommit, int collectionID, String collectionName, HashMap<String, String> disponibilite, String defaultDocumentType) throws ClassNotFoundException, SQLException, IndexorException
	{
		//INIT MySQL Connector
		this.downloadDB = "jdbc:mysql://" + downloadDB + "?autoReconnect=true";
		RFHarvesterLogger.info("Initializing " + this.getClass().getName());
		String jdbc = "Fatal error NOJDBC";
		jdbc = "com.mysql.jdbc.Driver";
		RFHarvesterLogger.debug("Init " + jdbc);
		Class.forName(jdbc);
		downloadDBConnection = DriverManager.getConnection(this.downloadDB, downloadDBlogin, downloadDBpassword);
		RFHarvesterLogger.info("Connection with " + this.downloadDB + " established");

		//INIT SOLR connector
		RFHarvesterLogger.info("Initializing " + this.getClass().getName());
		String zookeepersList = zookeepers.toString().substring(1, zookeepers.toString().length()-1).replaceAll(" ", "");

		client = new CloudSolrClient(zookeepersList);//zookeepersList);
		System.out.println("Connection with " + client.getZkHost() + " established");
		client.setDefaultCollection("notices");

		this.collectionID = collectionID;
		this.collectionName = collectionName;
		this.defaultDocumentType = defaultDocumentType;
		this.disponibilite = disponibilite;
		this.recomandedCommit=recomandedCommit;
	}

	public String insertRow(HashMap<String, ArrayList<String>> row) throws IndexorException
	{
		String out = null;

		SolrInputDocument document = new SolrInputDocument();

		document.addField("id", row.get("oai_identifier").get(0)+";"+collectionID);
		document.addField("collection_id", collectionID);
		document.addField("collection_name", collectionName);
		if(row.containsKey("controls_id"))
			document.addField("controls_id", row.get("controls_id").get(0));
		else
			throw new IndexorException("Missing collectionID in "+ row.toString());

		ArrayList<String> titles = new ArrayList<String>();
		if(row.containsKey("dc_title"))
			titles.addAll(row.get("dc_title"));
		else
			throw new IndexorException("Missing key dc_title in " + row);

		if(row.containsKey("dc_relation"))
			titles.addAll(row.get("dc_relation"));

		String titleString = RFHarvesterUtilities.arrayListToString(titles, "; ");
		if(titleString == null)
			throw new IndexorException("Missing titles in " + row);
		document.addField("title", titleString);
		document.addField("title_sort", normalizeTitleSort(titleString));

		if(row.containsKey("dc_subject"))
			document.addField("subject", RFHarvesterUtilities.arrayListToString(row.get("dc_subject "), "; "));

		ArrayList<String> creators = new ArrayList<String>();

		if(row.containsKey("dc_creator"))
			creators.addAll(row.get("dc_creator"));
		if(row.containsKey("dc_contributor"))
			creators.addAll(row.get("dc_contributor"));

		document.addField("creator", RFHarvesterUtilities.arrayListToString(creators, "; "));

		ArrayList<String> keywords = RFHarvesterUtilities.createSet(row.get("dc_title"), row.get("dc_creator"), row.get("dc_subject"), row.get("dc_description"), row.get("dc_publisher"), row.get("dc_contributor"), row.get("dc_coverage"), row.get("dc_relation"), row.get("dc_type"));
		String keywordsString = RFHarvesterUtilities.arrayListToString(keywords, "; ");
		document.addField("keyword", keywordsString);
		document.addField("autocomplete", keywordsString);

		if(row.containsKey("OAI_defaultDocumentType"))
		{
			document.addField("document_type", row.get("OAI_defaultDocumentType").get(0));
		}
		else if(row.containsKey("dc_type"))
		{
			document.addField("document_type", RFHarvesterUtilities.arrayListToString(row.get("dc_type"), ", "));
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

		if(row.containsKey("dc_language"))
			document.addField("lang_exact", RFHarvesterUtilities.arrayListToString(row.get("dc_language"), "; "));

		if(row.containsKey("dc_date"))
		{
			String dateDocument = null;
			try
			{
				Date date = simpleDateParse(row.get("dc_date").get(0));
				dateDocument = DF.format(date).toString();
			}
			catch (ParseException e)
			{
				RFHarvesterLogger.warning("Unable to parse in date format: " + row.get("dc_date").get(0) + " for " + row.get("oai_identifier").get(0) + " - " + row.get("dc_title") + RFHarvesterLogger.exceptionToString(e));
				try
				{
					document.addField("date_document", simpleDateParse("1000-01-01T23:59:59Z"));
				}
				catch (ParseException e1)
				{
					RFHarvesterLogger.warning("Unable to parse in date format: 1000-01-01T23:59:59Z for " + row.get("oai_identifier").get(0) + " - " + row.get("dc_title") + RFHarvesterLogger.exceptionToString(e1));
				}
			}
			if(dateDocument != null)
				document.addField("date_document", dateDocument);
//			if(collectionID == 5)
//				document.setDocumentBoost(dateBoost(dateDocument) + Integer.parseInt(row.get("solr_boost").get(0)));
		}
		else if(collectionID == 5)
		{
			document.setDocumentBoost(Integer.parseInt(row.get("solr_boost").get(0)));
		}
		else
		{
			try
			{
				document.addField("date_document", simpleDateParse("1000-01-01T23:59:59Z"));
			}
			catch (ParseException e1)
			{
				RFHarvesterLogger.warning("Unable to parse in date format: 1000-01-01T23:59:59Z for " + row.get("OAI_ID").get(0) + " - " + row.get("titres") + RFHarvesterLogger.exceptionToString(e1));
				throw new IndexorException();
			}
		}

		lastDocument = document;

		notices.add(document);

		if(notices.size() >= recomandedCommit)
		{
			push();
		}
		return out;
	}

	public void indexUploads() throws IndexorException
	{
		String query = "";
		HashSet<String> columns = new HashSet<String>();

		initTable();
		initTodayDate();

		try
		{
			ResultSet metadatasColumns;
			metadatasColumns = downloadDBConnection.getMetaData().getColumns(null, null, "metadatas", null);
			while(metadatasColumns.next())
			{
				columns.add(metadatasColumns.getString(4));
			}
			metadatasColumns.close();
			ResultSet controlsColumns;
			controlsColumns = downloadDBConnection.getMetaData().getColumns(null, null, "controls", null);
			while(controlsColumns.next())
			{
				columns.add(controlsColumns.getString(4));
			}
			controlsColumns.close();
		}
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		System.out.println(columns);

		query = "SELECT * FROM metadatas INNER JOIN controls ON metadatas.controls_id = controls.id WHERE metadatas.collection_id = " + collectionID;
//		query = "SELECT * FROM " + localTableName + " LIMIT 100000";
//		query = "SELECT * FROM " + localTableName + " LIMIT 400000  OFFSET 200000";
//		query = "SELECT * FROM " + localTableName + " WHERE dc_identifier = '1309585'";
//		query = "SELECT * FROM " + localTableName + " WHERE dc_identifier = '331752' OR dc_identifier = '331753' OR dc_identifier = '331754'";
		Statement downloadDBStatement = null;

		int type = ResultSet.TYPE_FORWARD_ONLY;
		int mode = ResultSet.CONCUR_READ_ONLY;
		try
		{
			downloadDBStatement = downloadDBConnection.createStatement(type, mode);
			ResultSet storedNotices = downloadDBStatement.executeQuery(query);

			int i = 1;
			while(storedNotices.next())
			{
				//System.out.println(i++ + " : ");
				//Build a row from datas getted in MySQL
				HashMap<String, ArrayList<String>> row = new HashMap<String, ArrayList<String>>();
				
				for(String column : columns)
				{
					String val = storedNotices.getString(column);
					if(val == null || val == "")
						continue;
					ArrayList<String> values = new ArrayList<String>();
					for(String value : val.split("; "))
					{
						try
						{
							/*if(column.compareTo("dc_title")==0)
							{
								System.out.println("\tDefault : " + value);
								System.out.println("\tUTF-8 : " + new String(value.getBytes("8859_1"), "UTF-8"));
								System.out.println("\tISO-8859-1 : " + new String(value.getBytes("8859_1"), "ISO-8859-1"));
								System.out.println("###");
							}*/
							values.add(new String(value.getBytes("8859_1"), "UTF-8"));
						}
						catch (UnsupportedEncodingException e)
						{
							RFHarvesterLogger.error("Unable to transform String(" + value + ") to UTF-8: " + row);
						}
					}
					row.put(column, values);
				}
				if(defaultDocumentType != null)
				{
					ArrayList<String> OAIDefaultDocumentType = new ArrayList<String>();
					OAIDefaultDocumentType.add(defaultDocumentType);
					row.put("OAI_defaultDocumentType", OAIDefaultDocumentType);
				}


				/*ArrayList<String> OAI_ID = new ArrayList<String>();
				OAI_ID.add(storedNotices.getString("oai_identifier"));
				row.put("OAI_ID", OAI_ID);

				ArrayList<String> controlsID = new ArrayList<String>();
				controlsID.add(storedNotices.getString("controls_id"));
				row.put("controlsID", controlsID);*/



				//Send the built row to SOLR

				try
				{
					insertRow(row);
				}
				catch (IndexorException e)
				{
					RFHarvesterLogger.warning(RFHarvesterLogger.exceptionToString(e));
				}
			}
		}
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		push();
		commit();
	}

	public void push()
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
		RFHarvesterLogger.debug("SOLR inserts finished with status " + responseAdding.getStatus());// + " and last " + responseAdding.getQTime() + " milliseconds");
	}

	public void commit() throws IndexorException
	{
		try
		{
			RFHarvesterLogger.info("Commit Solr modifications");
			client.commit();
			RFHarvesterLogger.info("SOLR commited");
		}
		catch(SolrServerException | IOException e)
		{
			RFHarvesterLogger.error(RFHarvesterLogger.exceptionToString(e));
		}
	}

	private Date simpleDateParse(String date) throws ParseException
	{
		if(date==null || date.isEmpty())
			return null;
		date = date.replaceAll("[^\\d]", "");
		date += "T23:00:00Z";
		SimpleDateFormat ymd = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss'Z'");
		SimpleDateFormat ym  = new SimpleDateFormat("yyyyMM'T'HH:mm:ss'Z'");
		SimpleDateFormat y   = new SimpleDateFormat("yyyy'T'HH:mm:ss'Z'");
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

	private String normalizeTitleSort(final String in)
	{
		String out = in.replaceAll("[^a-z^A-Z^à^á^â^ã^ä^å^ò^ó^ô^õ^ö^ø^è^é^ê^ë^ç^ì^í^î^ï^ù^ú^û^ü^ÿ^ñ^_^0-9^\\)\\(^\\]^\\[^\\\"^;^[:punct:]]$", "").trim();
		return out;
	}

	@Override
	public void end() throws IndexorException
	{
		push();
		commit();
	}
}
