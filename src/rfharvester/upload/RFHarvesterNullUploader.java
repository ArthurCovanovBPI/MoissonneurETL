package rfharvester.upload;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author ArthurCovanov
 */
public class RFHarvesterNullUploader implements RFHarvesterUploaderV2Interface
{
	public RFHarvesterNullUploader()
	{
	}

	public void dropLast()
	{
	}

	public String insertRow(final HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2Exception
	{
		String out = null;

		return out;
	}

	public void end() throws RFHarvesterUploaderV2Exception
	{
		
	}
}