package rfharvester.upload;

import java.util.ArrayList;
import java.util.HashMap;

import rfharvester.logger.RFHarvesterLogger;

/**
 * @author ArthurCovanov
 */
public class RFHarvesterUploaderV2Bundle implements RFHarvesterUploaderV2Interface
{
	private ArrayList<RFHarvesterUploaderV2Interface> uploaders = null;

	public RFHarvesterUploaderV2Bundle(ArrayList<RFHarvesterUploaderV2Interface> uploaders)
	{
		if(uploaders != null)
			this.uploaders = uploaders;
	}

	public RFHarvesterUploaderV2Bundle(RFHarvesterUploaderV2Interface ... uploaders)
	{
		if(uploaders != null && uploaders.length > 0)
		{
			this.uploaders = new ArrayList<RFHarvesterUploaderV2Interface>();
			for(RFHarvesterUploaderV2Interface uploader : uploaders)
			{
				if(uploader != null)
					this.uploaders.add(uploader);
			}
			if(this.uploaders.size() <= 0)
				uploaders = null;
		}
	}

	public void dropLast()
	{
		//Then we insert in other uploaders
		for(RFHarvesterUploaderV2Interface uploader : uploaders)
		{
			uploader.dropLast();
		}
	}

	public String insertRow(final HashMap<String, ArrayList<String>> row) throws RFHarvesterUploaderV2ClassException
	{
		String out = null;
		String in = null;
		ArrayList<RFHarvesterUploaderV2Interface> success = new ArrayList<RFHarvesterUploaderV2Interface>();

		try
		{
			if(uploaders != null)
			{
				//Special case for control, we need control's ID to insert in other tables
				for(RFHarvesterUploaderV2Interface uploader : uploaders)
				{
					if(uploader.getClass().toString().compareTo(UploadControlsMySQL5V2.class.toString())==0)
					{
						in = uploader.insertRow(row);
						success.add(uploader);
					}
				}
				//Then we insert in other uploaders
				for(RFHarvesterUploaderV2Interface uploader : uploaders)
				{
					if(uploader.getClass().toString().compareTo(UploadControlsMySQL5V2.class.toString())!=0)
					{
						if(in!=null)
						{
							ArrayList<String> controlID = new ArrayList<String>();
							controlID.add(in);
							row.put("controlID", controlID);
						}
						uploader.insertRow(row);

						success.add(uploader);
					}
				}
			}
		}
		catch(RFHarvesterUploaderV2ClassException e)
		{
			for(RFHarvesterUploaderV2Interface s : success)
			{
				s.dropLast();
				RFHarvesterLogger.debug("DROP LAST FROM " + s.getClass().getName());
			}
			throw e;
		}
		return out;
	}

	public void end() throws RFHarvesterUploaderV2ClassException
	{
		if(uploaders != null)
		{
			//Special case for control, we need control's ID to insert in other tables
			for(RFHarvesterUploaderV2Interface uploader : uploaders)
			{
				if(uploader.getClass().toString().compareTo(UploadNoticesSolrV2.class.toString())==0)
				{
					uploader.end();
				}
			}
			//Then we insert in other uploaders
			for(RFHarvesterUploaderV2Interface uploader : uploaders)
			{
				if(uploader.getClass().toString().compareTo(UploadNoticesSolrV2.class.toString())!=0)
				{
					uploader.end();
				}
			}
		}
	}
}