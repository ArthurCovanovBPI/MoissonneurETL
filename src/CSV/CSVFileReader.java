package CSV;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class CSVFileReader
{
	private String separator;

	private FileInputStream fin;
	private InputStreamReader isr;
	private BufferedReader br;

	private String columns[];

	private int line = 0;

	public CSVFileReader(String filePath, String separator) throws FileNotFoundException, IOException
	{
		this.separator = separator;

		this.fin = new FileInputStream(filePath);
		this.isr = new InputStreamReader(fin);
		this.br = new BufferedReader(isr);

		line++;

		String firstLine = br.readLine();

		this.columns = firstLine.split(separator);
	}

	public HashMap<String, ArrayList<String>> nextLine() throws CSVFileReaderException
	{
		HashMap<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();

		line++;

		String nextLine;

		String values[];
		try
		{
			nextLine = br.readLine();
			values = nextLine.split(separator);
		}
		catch (IOException e)
		{
			throw new CSVFileReaderException(e);
		}

		if(values.length != columns.length)
			throw new CSVFileReaderException("Values count different from column count at line " + line);

		for(int i = 0; i< columns.length; i++)
		{
			ArrayList<String> value = new ArrayList<String>();
			value.add(values[i]);
			result.put(columns[i], value);
		}

		return result;
	}
}
