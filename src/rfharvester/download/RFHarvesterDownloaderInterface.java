package rfharvester.download;

import java.util.ArrayList;

import rfharvester.upload.RFHarvesterUploaderInterface;

/**
 * @author ArthurCovanov
 */
public interface RFHarvesterDownloaderInterface
{
	/**
	 * @param millis Harvest all data since Date.getTime()<br/>
	 *        if(millis <= 0) then full harvest.<br/>
	 *        millis==0 <=> Date is 01/01/1970 - 01:00:00.000
	 */
	public void download(ArrayList<RFHarvesterUploaderInterface> UploadsList, long millis);
}