package rfharvester.upload;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class UploadAuthoritiesSolr implements RFHarvesterUploaderInterface
{
//	private final String SOLRurl = "http://10.1.2.140:8080/solr/";
	private final String SOLRurl = "http://10.1.2.113:8080/solr/";
	private final String SOLRauthoritiesURL = SOLRurl + "authorities/";
	private final String SOLRauthorities_newURL = SOLRurl + "authorities_new/";
	private SolrClient solrcore = null;
	private SolrClient solrauthorities = null;
	private SolrClient solrauthorities_new = null;

	SimpleDateFormat harvestingDateFormatter = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat dateDocumentFormatter = new SimpleDateFormat("yyyy");

    Collection<SolrInputDocument> authorities = new ArrayList<SolrInputDocument>();

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
	}

	public void copyIntoOld()
	{
	}

	public UploadAuthoritiesSolr()
	{
		RFHarvesterLogger.info("Initializing " + className);
		solrcore = new HttpSolrClient(SOLRurl);
		RFHarvesterLogger.info("Connection with " + SOLRurl + " established");
		solrauthorities = new HttpSolrClient(SOLRauthoritiesURL);
		RFHarvesterLogger.info("Connection with " + SOLRauthoritiesURL + " established");
		solrauthorities_new = new HttpSolrClient(SOLRauthorities_newURL);
		RFHarvesterLogger.info("Connection with " + SOLRauthorities_newURL + " established");

//		try
//		{
//			RFHarvesterLogger.debug("solrauthorities_new.deleteByQuery(\"*:*\");");
//			UpdateResponse SOLRresponse = solrauthorities_new.deleteByQuery("*:*");
//			RFHarvesterLogger.debug(SOLRresponse.toString());
//			solrauthorities_new.commit();
//
////			initTables();
//		}
//		catch(SolrServerException | IOException e)
//		{
//			e.printStackTrace();
//			System.exit(0); // Program won't run with uninitialized SOLR.
//		}
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
		document.addField("seq_no", rows.get("solr_seq_no"));
		String retenu = rows.get("solr_retenu");
		document.addField("retenu", (retenu != null)? retenu.split(" @;@ ") : null);
		String rejete = rows.get("solr_rejete");
		document.addField("rejete", (rejete != null)? rejete.split(" @;@ ") : null);
		String relation = rows.get("solr_relation");
		document.addField("relation", (relation != null)? relation.split(" @;@ ") : null);

		authorities.add(document);
		storedRows++;
	}

	public void storeErrorIdentifier(String dc_identifier)
	{
	}
	public void cleanErrors()
	{
	}

	@Override
	public void commit()
	{
		UpdateResponse responseAdding = null;
		try
		{
			responseAdding = solrauthorities_new.add(authorities);
			solrauthorities_new.commit(true, true);
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
		authorities.clear();
		storedRows=0;
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
			RFHarvesterLogger.debug("solrauthorities.deleteByQuery(\"*:*\");");
			UpdateResponse SOLRresponse = solrauthorities.deleteByQuery("(*:*)");
			RFHarvesterLogger.debug(SOLRresponse.toString());
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
			System.exit(0); // Program won't run with uninitialized SOLR.
		}
		try
		{
			String[] solrindexesdirs = new String[]{};
			String[] solrcores = new String[]{"authorities_new"};
			RFHarvesterLogger.debug("Merging solrcores " + solrcores[0] + " into authorities");
			CoreAdminResponse mergeResponse = CoreAdminRequest.mergeIndexes("authorities", solrindexesdirs, solrcores, solrcore);
			RFHarvesterLogger.debug(mergeResponse.getResponse().toString());
			solrauthorities.commit();
			RFHarvesterLogger.debug("SOLR commited");
		}
		catch(SolrServerException | IOException e)
		{
			e.printStackTrace();
		}
	}
}
