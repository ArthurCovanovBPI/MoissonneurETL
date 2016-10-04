package rfharvester.upload;

import java.util.HashMap;

/**
 * @author ArthurCovanov
 *
 */
public interface RFHarvesterUploaderInterface
{
	public int getStoredRows();
	public int getRecomandedCommit();
	public String getClassName();
	public void initTable();
	public void copyIntoOld();
	public void begin();
	public void insertRow(final HashMap<String, String> rows) throws RFHarvesterUploaderException;
	public void storeErrorIdentifier(String dc_identifier);
	public void cleanErrors();
	public void commit();
	public void mergeOldTable();
	public void replaceOldTable();
}