package rfharvester.transformator;

import java.util.HashMap;

import rfharvester.download.AuthoritiesDownloaderException;

/**
 * BPI Harvester storage class. All classes, that implement
 * RFHarvesterUploaderInterface, must upload an instance of this class.
 * 
 * @author ArthurCovanov
 */
public final class RFHarvesterAuthoritiesTransfo extends HashMap<String, String>
{
	private static final long serialVersionUID = -5220477509918877309L;
	
	public void transform() throws AuthoritiesDownloaderException
	{
//		ExecutorService es = Executors.newCachedThreadPool();
//
//		es.execute(new Runnable()
//		{
//			public void run()
//			{
//			}
//		});
//		es.shutdown();

		//For SOLR statement
		this.put("solr_seq_no", this.get("seq_no"));
		this.put("solr_retenu",  this.get("retenu"));
		this.put("solr_rejete", this.get("rejete"));
		this.put("solr_relation",  this.get("relation"));
	}
}