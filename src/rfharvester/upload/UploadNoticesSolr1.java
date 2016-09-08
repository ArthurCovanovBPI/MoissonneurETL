package rfharvester.upload;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;

import rfharvester.logger.RFHarvesterLogger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class UploadNoticesSolr1 implements RFHarvesterUploaderInterface
{
//	private final String SOLRurl = "http://10.1.2.140:8080/solr/";
	private final String SOLRurl = "http://10.1.2.114:8080/solr/";
	private final String SOLRnoticesURL = SOLRurl + "notices/";
	private final String SOLRnotice_newURL = SOLRurl + "notices_new/";
//	private final String SOLRnotice_oldURL = SOLRurl + "notices_old/";
	private SolrClient solrcore = null;
	private SolrClient solrnotices = null;
	private SolrClient solrnotices_new = null;
//	private SolrClient solrnotices_old = null;

	SimpleDateFormat harvestingDateFormatter = new SimpleDateFormat("yyyyMMdd", Locale.US);
	SimpleDateFormat dateDocumentFormatter = new SimpleDateFormat("yyyyMMdd", Locale.FRANCE);

	Collection<SolrInputDocument> notices = new ArrayList<SolrInputDocument>();

	private final int collection_id;
	private int storedRows = 0;
	private int recomandedCommit = 20000;
	private final String className = this.getClass().getName();

	public int getStoredRows()
	{
		return storedRows;
	}

	public int getRecomandedCommit()
	{
		return recomandedCommit;
	}

	public String getClassName()
	{
		return className;
	}

	public void initTable()
	{
		try
		{
			RFHarvesterLogger.debug("solrnotices_new.deleteByQuery(\"*:*\");");
			UpdateResponse SOLRresponse = solrnotices_new.deleteByQuery("*:*");
			RFHarvesterLogger.debug(SOLRresponse.toString());
			solrnotices_new.commit();

//			RFHarvesterLogger.debug("solrnotices.deleteByQuery(\"*:*)\");");
//			SOLRresponse = solrnotices.deleteByQuery("*:*");
//			RFHarvesterLogger.debug(SOLRresponse.toString());
//			solrnotices.commit();

//			RFHarvesterLogger.debug("solrnotices.deleteByQuery(\"-(collection_id:(5)))\");");
//			SOLRresponse = solrnotices.deleteByQuery("-(collection_id:(5))");
//			RFHarvesterLogger.debug(SOLRresponse.toString());
//			solrnotices.commit();
			
//			RFHarvesterLogger.debug("solrnotices_old.deleteByQuery(\"*:*\");");
//			SOLRresponse = solrnotices_old.deleteByQuery("*:*");
//			RFHarvesterLogger.debug(SOLRresponse.toString());
//			solrnotices_old.commit();
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
	}

	public void copyIntoOld()
	{
//		try
//		{
//
////			String[] solrindexesdirs = new String[] {};
////			String[] solrcores = new String[] { "notices" };
////			RFHarvesterLogger.info("Merging solrcores " + solrcores[0] + " into notices_old");
////			CoreAdminResponse mergeResponse = CoreAdminRequest.mergeIndexes("notices_old", solrindexesdirs, solrcores, solrcore);
////			RFHarvesterLogger.debug(mergeResponse.getResponse().toString());
////			RFHarvesterLogger.info("Commit Solr_old modifications");
////			solrnotices_old.commit();
//			RFHarvesterLogger.info("SOLR_old commited");
//		}
//		catch(SolrServerException | IOException e)
//		{
//			e.printStackTrace();
//			System.exit(0); // Program won't run with uninitialized SOLR.
//		}
	}

	public UploadNoticesSolr1(int collectionId)
	{
		RFHarvesterLogger.info("Initializing " + className);
		solrcore = new HttpSolrClient(SOLRurl);
		RFHarvesterLogger.info("Connection with " + SOLRurl + " established");
		solrnotices = new HttpSolrClient(SOLRnoticesURL);
		RFHarvesterLogger.info("Connection with " + SOLRnoticesURL + " established");
		solrnotices_new = new HttpSolrClient(SOLRnotice_newURL);
		RFHarvesterLogger.info("Connection with " + SOLRnotice_newURL + " established");
//		solrnotices_old = new HttpSolrClient(SOLRnotice_oldURL);
//		RFHarvesterLogger.info("Connection with " + SOLRnotice_oldURL + " established");
		collection_id = collectionId;
	}

	@Override
	public void begin()
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void insertRow(final HashMap<String, String> rows)
	{

		SolrInputDocument document = new SolrInputDocument();
		try
		{
//			TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
			document.addField("id", new String(rows.get("solr_id").getBytes(), "UTF-8"));
			document.addField("collection_id", new String(rows.get("solr_collection_id").getBytes(), "UTF-8"));
			document.addField("controls_id", new String(rows.get("solr_controls_id").getBytes(), "UTF-8"));
			document.addField("collection_name", new String(rows.get("solr_collection_name").getBytes(), "UTF-8"));
			document.addField("title", new String(rows.get("solr_title").getBytes(), "UTF-8"));
			document.addField("creator", new String(rows.get("solr_creator").getBytes(), "UTF-8"));
			document.addField("subject", new String(rows.get("solr_subject").getBytes(), "UTF-8"));
			document.addField("description", new String(rows.get("solr_description").getBytes(), "UTF-8"));
			document.addField("publisher", new String(rows.get("solr_publisher").getBytes(), "UTF-8"));
			document.addField("keyword", new String(rows.get("solr_keyword").getBytes(), "UTF-8"));
			document.addField("theme", new String(rows.get("solr_theme").getBytes(), "UTF-8").split(";"));
			document.addField("theme_rebond", new String(rows.get("solr_theme_rebond").getBytes(), "UTF-8"));
			document.addField("document_type", new String(rows.get("solr_document_type").getBytes(), "UTF-8"));
//			try
//			{
//				document.addField("harvesting_date", harvestingDateFormatter.parse(rows.get("solr_harvesting_date")));
//				System.out.println("solr_harvesting_date : " + rows.get("solr_harvesting_date"));
//				System.out.println("harvesting_date : " + document.get("harvesting_date"));
//			}
//			catch(ParseException e)
//			{
//				RFHarvesterLogger.error("Unable to parse harvesting_date: (" + rows.get("solr_id") + ", " + rows.get("solr_title") + ", " + rows.get("solr_harvesting_date") + ")");
//			}
			if(rows.get("solr_harvesting_date") != null)
				document.addField("harvesting_date", rows.get("solr_harvesting_date"));
			document.addField("isbn", new String(rows.get("solr_isbn").getBytes(), "UTF-8"));
			document.addField("issn", new String(rows.get("solr_issn").getBytes(), "UTF-8"));

//			String[] cotes = rows.get("solr_cote_rebond").split(", ");
			String[] cotes = (new String(rows.get("solr_cote_rebond").getBytes(), "UTF-8")).split(", ");
//			document.addField("cote_rebond", new String(rows.get("solr_cote_rebond").getBytes(), "UTF-8"));
			document.addField("cote_rebond", cotes);

//			String[] bdm_info = rows.get("solr_bdm_info").split(", ");
			String[] bdm_info = (new String(rows.get("solr_bdm_info").getBytes(), "UTF-8")).split(", ");
//			document.addField("bdm_info", new String(rows.get("solr_bdm_info").getBytes(), "UTF-8"));
			document.addField("bdm_info", bdm_info);
			document.addField("autocomplete", new String(rows.get("solr_autocomplete").getBytes(), "UTF-8"));
			document.addField("autocomplete_creator", new String(rows.get("solr_autocomplete_creator").getBytes(), "UTF-8"));
			document.addField("autocomplete_publisher", new String(rows.get("solr_autocomplete_publisher").getBytes(), "UTF-8"));

			document.addField("autocomplete_theme", new String(rows.get("solr_autocomplete_theme").getBytes(), "UTF-8"));
			document.addField("autocomplete_title", new String(rows.get("solr_autocomplete_title").getBytes(), "UTF-8"));
			document.addField("autocomplete_subject", new String(rows.get("solr_autocomplete_subject").getBytes(), "UTF-8"));
			document.addField("autocomplete_description", new String(rows.get("solr_autocomplete_description").getBytes(), "UTF-8"));
			document.addField("barcode", new String(rows.get("solr_barcode").getBytes(), "UTF-8"));
			document.addField("indice", new String(rows.get("solr_indice").getBytes(), "UTF-8"));
			document.addField("custom_document_type", new String(rows.get("solr_custom_document_type").getBytes(), "UTF-8"));
			document.addField("title_sort", new String(rows.get("solr_title_sort").getBytes(), "UTF-8"));
			document.addField("lang_exact", new String(rows.get("solr_lang_exact").getBytes(), "UTF-8"));
//			try
//			{
//				if(rows.get("solr_date_document").length()>0)
//					document.addField("date_document", dateDocumentFormatter.parse(rows.get("solr_date_document")));
//				System.out.println("solr_date_document : " + rows.get("solr_date_document"));
//				System.out.println("date_document : " + document.get("date_document"));
//			}
//			catch(ParseException e)
//			{
//				RFHarvesterLogger.error("Unable to parse solr_date_document: (" + rows.get("solr_id") + ", " + rows.get("solr_title") + ", " + rows.get("solr_date_document") + ")");
//			}
			if(rows.get("solr_date_document") != null)
				document.addField("date_document", rows.get("solr_date_document"));
			else
				document.addField("date_document", "1000-01-02T00:00:00Z");
			document.addField("dispo_sur_poste", new String(rows.get("solr_dispo_sur_poste").getBytes(), "UTF-8"));
			document.addField("dispo_bibliotheque", new String(rows.get("solr_dispo_bibliotheque").getBytes(), "UTF-8"));
			document.addField("dispo_access_libre", new String(rows.get("solr_dispo_access_libre").getBytes(), "UTF-8"));
			document.addField("dispo_avec_reservation", new String(rows.get("solr_dispo_avec_reservation").getBytes(), "UTF-8"));
			document.addField("dispo_broadcast_group", ((rows.get("solr_dispo_broadcast_group") == null) ? "" : new String(rows.get("solr_dispo_broadcast_group").getBytes(), "UTF-8").split(";")));
			document.addField("rights", new String(rows.get("solr_rights").getBytes(), "UTF-8"));
//			String[] locations = rows.get("solr_location").split(", ");
			String[] locations = (new String(rows.get("solr_location").getBytes(), "UTF-8")).split("@;@");
//			document.addField("location", new String(rows.get("solr_location").getBytes(), "UTF-8"));
			document.addField("location", locations);
			document.addField("coverage_spatial", new String(rows.get("solr_coverage_spatial").getBytes(), "UTF-8"));
			if(rows.get("solr_date_end_new") != null)
				document.addField("date_end_new", new String(rows.get("solr_date_end_new").getBytes(), "UTF-8"));
			if(rows.get("solr_commercial_number") != null)
				document.addField("commercial_number", new String(rows.get("solr_commercial_number").getBytes(), "UTF-8"));
			if(rows.get("solr_musical_kind") != null)
				document.addField("musical_kind", new String(rows.get("solr_musical_kind").getBytes(), "UTF-8"));
//			document.addField("boost", rows.get("solr_boost"));
			document.setDocumentBoost(Integer.parseInt(rows.get("solr_boost")));
		}
		catch(UnsupportedEncodingException e)
		{
			RFHarvesterLogger.error("Unable to convert to UTF-8: " + rows.get("solr_id"));
		}
		catch(Exception e)
		{
			e.printStackTrace();

			for(String k : rows.keySet())
			{
				if(k.contains("solr"))
					System.out.println(k + " - " + rows.get(k));
			}
			System.exit(0);
		}
		notices.add(document);
		storedRows++;
	}

	HashSet<String> errors = new HashSet<String>();

	public void storeErrorIdentifier(String dc_identifier)
	{
		errors.add(dc_identifier + ";" + collection_id);
	}

	public void cleanErrors()
	{
		for(String error : errors)
		{
			try
			{
				RFHarvesterLogger.info("solrnotices_new.deleteByQuery(\"id:(" + error + ")\");");
				UpdateResponse SOLRresponse = solrnotices_new.deleteByQuery("id:(" + error + ")");
				RFHarvesterLogger.debug(SOLRresponse.toString());
			}
			catch(SolrServerException | IOException e)
			{
				e.printStackTrace();
				System.exit(0); // Program won't run with uninitialized SOLR.
			}
		}
	}

	@Override
	public void commit()
	{
		UpdateResponse responseAdding = null;
		try
		{
//			long startTime = System.currentTimeMillis();
//			solrnotices_new = new HttpSolrClient("http://10.1.2.140:8080/solr/notices_new");
//			SolrQuery qt = new SolrQuery();
//			qt.setParam("charset", "ISO-8859-1");
////			System.out.println(qt.toNamedList());
////			solrnotices_new.query(qt);
//			
//			UpdateRequest req = new UpdateRequest();
//			System.out.println(req.getQueryParams());
//			System.out.println(req.getParams());
//			req.setQueryParams(qt.getParameterNames());
//			req.setParam("charset", "ISO-8859-1");
//			System.out.println(req.getQueryParams());
//			System.out.println(req.getParams());
//			req.setAction(UpdateRequest.ACTION.COMMIT, false, false);
//			req.add(notices);
//			
//			System.out.println(ClientUtils.toQueryString(qt, false));
//			UpdateResponse rsp = req.process(solrnotices_new);
//			System.out.print ("Added documents to solr. Time taken = " + rsp.getElapsedTime() + ". " + rsp.toString());
//			long endTime = System.currentTimeMillis();
//			System.out.println (" , time-taken=" + ((double)(endTime-startTime))/1000.00 + " seconds");

			if(notices.size()<=0)
			{
				RFHarvesterLogger.warning("Empty SOLR notices set");
				return;
			}
			responseAdding = solrnotices_new.add(notices);
			solrnotices_new.commit(true, true);
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
		notices.clear();
		storedRows = 0;
		RFHarvesterLogger.debug("SOLR commit finished with status " + responseAdding.getStatus() + " and last " + responseAdding.getQTime() + " milliseconds");
	}

	@Override
	public void mergeOldTable()
	{
		RFHarvesterLogger.info(this.className + ".mergeOldTable() finished successfully");
	}

	@Override
	public void replaceOldTable()
	{
		try
		{
			RFHarvesterLogger.info("solrnotices.deleteByQuery(\"collection_id:(" + collection_id + ")\");");
			UpdateResponse SOLRresponse = solrnotices.deleteByQuery("collection_id:(" + collection_id + ")");
			RFHarvesterLogger.debug(SOLRresponse.toString());
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
		try
		{
			String[] solrindexesdirs = new String[] {};
			String[] solrcores = new String[] { "notices_new" };
			RFHarvesterLogger.info("Merging solrcores " + solrcores[0] + " into notices");
			CoreAdminResponse mergeResponse = CoreAdminRequest.mergeIndexes("notices", solrindexesdirs, solrcores, solrcore);
			RFHarvesterLogger.debug(mergeResponse.getResponse().toString());
			RFHarvesterLogger.info("Commit Solr modifications");
			solrnotices.commit();
			RFHarvesterLogger.info("SOLR commited");
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void restoreOldTable()
	{
		try
		{
			RFHarvesterLogger.info("solrnotices.deleteByQuery(\"*:*\");");
			UpdateResponse SOLRresponse = solrnotices.deleteByQuery("*:*");
			RFHarvesterLogger.debug(SOLRresponse.toString());
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
		try
		{
			String[] solrindexesdirs = new String[] {};
			String[] solrcores = new String[] { "notices_old" };
			RFHarvesterLogger.info("Merging solrcores " + solrcores[0] + " into notices");
			CoreAdminResponse mergeResponse = CoreAdminRequest.mergeIndexes("notices", solrindexesdirs, solrcores, solrcore);
			RFHarvesterLogger.debug(mergeResponse.getResponse().toString());
			RFHarvesterLogger.info("Commit Solr modifications");
			solrnotices.commit();
			RFHarvesterLogger.info("SOLR commited");
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void postUpdateTable()
	{}
}
