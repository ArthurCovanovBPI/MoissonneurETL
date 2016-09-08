package rfharvester.transformator;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author ArthurCovanov
 */
public interface RFHarvesterTransformatorInterfaceV2
{
	public HashMap<String, ArrayList<String>> transform(HashMap<String, ArrayList<String>> source);
}