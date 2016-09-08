package rfharvester.upload;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author ArthurCovanov
 *
 */
public interface RFHarvesterUploaderV2Interface
{
	public String insertRow(final HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2ClassException;
	public void end() throws RFHarvesterUploaderV2ClassException;
	public void dropLast();
}