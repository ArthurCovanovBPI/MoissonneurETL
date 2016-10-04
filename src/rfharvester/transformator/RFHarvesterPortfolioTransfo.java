package rfharvester.transformator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import rfharvester.download.AuthoritiesDownloaderException;
import rfharvester.logger.RFHarvesterLogger;
import rfharvester.upload.RFHarvesterUploaderException;
import rfharvester.upload.RFHarvesterUploaderInterface;

/**
 * BPI Harvester storage class. All classes, that implement
 * RFHarvesterUploaderInterface, must upload an instance of this class.
 * 
 * @author ArthurCovanov
 */
public final class RFHarvesterPortfolioTransfo extends HashMap<String, String>
{
	/*
	 * You may think you know what the following code does.
	 * But you don't. Trust me.
	 * Fiddle with it, and you'll spend many a sleepless
	 * night cursing the moment you thought you'd be clever
	 * enough to "optimize" the code below.
	 * Now close this file and go play with something else.
	 */ 

	/**
	 * 
	 */
	private static final long serialVersionUID = -5220477509918877309L;

	private String normalizePortFolio(String word)
	{
		if(word == null)
			return "";
		String result = word.replace("@;@", "|LF_DEL|").replace(";", ",").replace(", , ", ", ").replace("/ , ", ", ").replace("|LF_DEL|", ";").replace(", ;", ",");
		if(result.endsWith(","))
		{
			result = result.substring(0, result.length() - 1);
		}
		if(result.endsWith("/"))
		{
			result = result.substring(0, result.length() - 1);
		}
		if(result.endsWith(";"))
		{
			result = result.substring(0, result.length() - 1);
		}
		if(result.endsWith("/"))
		{
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

	private String normalizeTitle(String word)
	{
		String result = normalizePortFolio(word).replace(" : ; ", " : ").replace(" : / ", " : ").replace(" / : ", " : ").replace(" / ; ", "; ").replace(" = ; ", "; ");
		if(result.endsWith(","))
		{
			result = result.substring(0, result.length() - 1);
		}
		if(result.endsWith("/;"))
		{
			result = result.substring(0, result.length() - 2);
		}
		if(result.endsWith(";"))
		{
			result = result.substring(0, result.length() - 1);
		}
		if(result.endsWith("/"))
		{
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

	private String dcformater(String dcformat)
	{
		String duration_regepx = "[0-9]{2}[0-5][0-9]([0-5][0-9])?";
		ArrayList<String> newdc = new ArrayList<String>();
		boolean matched = false;
		for(String dcformatsplit : dcformat.split(" ; "))
		{
			String t = dcformatsplit.trim();
			if(t.matches(duration_regepx))
			{
				matched = true;
				String h = t.substring(0, 2);
				String m = t.substring(2, 4);
				String s = (t.length() == 6) ? t.substring(4, 6) : "";
				h = (h.compareTo("00") != 0 && h != "") ? (h + " heure(s) ") : "";
				m = (m.compareTo("00") != 0 && m != "") ? (m + " minute(s) ") : "";
				s = (s.compareTo("00") != 0 && s != "") ? (s + " seconde(s)") : "";
				newdc.add((h + m + s).trim());
			}
			else
			{
				newdc.add(dcformatsplit.trim());
			}
		}
		if(matched)
		{
			String newdcformat = "";
			for(String dcformatsplited : newdc)
			{
				newdcformat += (" ; " + dcformatsplited);
			}
			if(newdcformat.length() > 0)
				newdcformat = newdcformat.substring(3);
			return newdcformat;
		}
		return dcformat;
	}

	public boolean check()
	{
		return true;
	}

	private String getCDU(HashMap<String, String> authorityindices, String indices)
	{
		if(indices == null || indices == "")
			return "";
		
		String label = "";
		for(String indice : indices.split("@;@"))
		{
			indice = indice.trim();
			label += ("@;@" + ((!authorityindices.containsKey(indice)) ? " " : authorityindices.get(indice)));
		}
		return label;
	}

	private String save_document_type(String dctype, HashMap<String, String> documenttypes, HashMap<String, String> primarydocumenttypes)
	{
		String doctype = null;
		try
		{
			doctype = documenttypes.get(dctype);
		}
		catch(Exception e)
		{
			RFHarvesterLogger.warning("Can't find " + dctype + " in documenttypes");
			return dctype;
		}
		try
		{
			return primarydocumenttypes.get(doctype);
		}
		catch(Exception e)
		{
			RFHarvesterLogger.warning("Can't find " + doctype + " in primarydocumenttypes");
			return dctype;
		}
	}
	
	private static HashSet<String> nonmatchedCduThemes = new HashSet<>();	
	private static HashSet<String> missingCduIndice = new HashSet<>();

	private String translateCduThemes(String themes, HashMap<String, String> authorityindices, HashSet<Integer> refsourcesLengthsReferences, HashMap<String, String> cduThemesReferences, HashSet<Integer> refsourcesLengthsExclusions, HashMap<String, String> cduThemesExclusions) throws AuthoritiesDownloaderException
	{
		HashSet<String> lf_themes = new HashSet<String>();
		final String THEME_SEPARATOR = " > ";

		for(String theme : themes.split(" @;@ "))
		{
			theme = theme.trim();

			String associatedLibelle = "";
			if(authorityindices.containsKey(theme))
			{
				associatedLibelle = authorityindices.get(theme);
				if(associatedLibelle == null) //TODO Should never happen, delete in case
				{
//					error = true;
					if(!missingCduIndice.contains(theme))
					{
						missingCduIndice.add(theme);
						RFHarvesterLogger.warning("No associated libelle to indice \"" + theme + "\" in dw_authorityindices table.\n                                                 "+ missingCduIndice.size() + " Missing indices found.");
					}
//					continue;
				}
			}

			HashSet<String> references = new HashSet<String>();
			for(int l : refsourcesLengthsReferences)
			{
				if(theme.length() < l)
					continue;
				String subStringedTheme = theme.substring(0, l);
				if(cduThemesReferences.containsKey(subStringedTheme))
				{
					references.add(cduThemesReferences.get(subStringedTheme));
				}
			}
//			System.out.println("references : " + references);

			HashSet<String> exclusions = new HashSet<String>();
			for(int l : refsourcesLengthsExclusions)
			{
				if(theme.length() < l)
					continue;
				String subStringedTheme = theme.substring(0, l);
				if(cduThemesExclusions.containsKey(subStringedTheme))
				{
					exclusions.add(cduThemesExclusions.get(subStringedTheme));
				}
			}
//			System.out.println("exclusions : " + exclusions);

			for(String exclusion : exclusions)
			{
				if(references.contains(exclusion))
					references.remove(exclusion);
			}

			if(references.size()<=0)
			{
				if(!nonmatchedCduThemes.contains(theme))
				{
					nonmatchedCduThemes.add(theme);
					RFHarvesterLogger.warning("Can't translateCduThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + theme + "\n                                                 Empty references.\n                                                 "+ nonmatchedCduThemes.size() + " missing Cdu themes");
				}
				continue;
			}

//			System.out.println("references : " + references);
			for(String reference : references)
			{
				if(reference!=null && reference.length()>0)
				{
					String lf_theme = reference;
					if(associatedLibelle!=null && associatedLibelle.length()>0)
					{
//						associatedLibelle = associatedLibelle.substring(0, 1).toUpperCase() + associatedLibelle.substring(1).toLowerCase();
						lf_theme += THEME_SEPARATOR + associatedLibelle;
					}
					String[] splited_lf_theme = lf_theme.split(" > ");
					lf_theme = "";
					for(int i = 0; i< splited_lf_theme.length; i++)
					{
						lf_theme += (" > " + splited_lf_theme[i].substring(0, 1).toUpperCase() + splited_lf_theme[i].substring(1).toLowerCase());
					}
					lf_themes.add(lf_theme.substring(3));
				}
			}
		}

		String result = "";
		if(lf_themes.size()<=0)
			return "";
		for(String lf_theme : lf_themes)
		{
			result+=(";" + lf_theme);
		}
		return result.substring(1);
	}
	
	private static HashSet<String> nonmatchedBdmThemes = new HashSet<>();
	
	private String translateBdmThemes(String multi_themes, String multi_labels, String source, HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesReferences, HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesExclusions) throws AuthoritiesDownloaderException
	{
		ArrayList<String> lf_themes = new ArrayList<String>();
		String errorMessage = "";
		final String THEME_SEPARATOR = " > ";
		ArrayList<String> multiThemes =  new ArrayList<String>(Arrays.asList(multi_themes.split("@;@")));
		int multiThemesLength = multiThemes.size();
		ArrayList<String> multiLabels =  new ArrayList<String>(Arrays.asList(multi_labels.split("@;@")));
		int multiLabelsLength = multiLabels.size();

		if( multiThemesLength <= 0)
			errorMessage += ("\n                                                 no bpi_theme found.");
		if(multiLabelsLength <= 0)
			errorMessage += ("\n                                                 no bpi_theme_lib found.");
		if(multiLabelsLength != multiThemesLength)
			errorMessage += ("\n                                                 multiLabelsLength != multiThemesLength");
		if(errorMessage.length()>0)
			throw new AuthoritiesDownloaderException(errorMessage);

		if(!bdmThemesReferences.containsKey(source))
			throw new AuthoritiesDownloaderException("Missing source '" + source + "' in themes_references.");
			
		HashMap<String, HashMap<String, String>> bdmThemesReferencesSourced = bdmThemesReferences.get(source);
		HashMap<String, HashMap<String, String>> bdmThemesExclusionsSourced = bdmThemesExclusions.get(source);
		
		for(int i = 0; i < multiThemesLength; i++)
		{
			//TODO Not optimal, but most understandable
			ArrayList<String> themes = new ArrayList<String>(Arrays.asList(multiThemes.get(i).split(" > ")));
			ArrayList<String> labels = new ArrayList<String>(Arrays.asList(multiLabels.get(i).split(" > ")));
			ArrayList<String> reversedThemes = new ArrayList<String>();
			for(int j = themes.size()-1; j >=0 ; j--)
				reversedThemes.add(themes.get(j).trim());

			HashMap<String, String> references = new HashMap<>();
			for(int j=0; references.isEmpty() && j < reversedThemes.size() && !reversedThemes.get(j).isEmpty(); j++)
			{
				String curtheme = reversedThemes.get(j);
				if(!bdmThemesReferencesSourced.containsKey(curtheme))
				{
					if(!nonmatchedBdmThemes.contains(curtheme))
					{
						nonmatchedBdmThemes.add(curtheme);
						RFHarvesterLogger.warning("Can't translateBdmThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("bpi_theme") + " ~ " + this.get("bpi_theme_lib") + "\n                                                 Missing theme '" + reversedThemes.get(j) + "' in source '" + source + "' in themes_references.\n                                                 "+ nonmatchedBdmThemes.size() + " missing Bdm themes");
					}
					continue;
				}
				references = bdmThemesReferencesSourced.get(curtheme);
				if(bdmThemesExclusionsSourced != null && !bdmThemesExclusionsSourced.isEmpty())
				{
					HashMap<String, String> exclusions = bdmThemesExclusionsSourced.get(curtheme);
					references.remove(exclusions.keySet());
				}
				
				int idx = (references.get("construction_mode").compareTo("F") == 0)? (j - 1) : j;
				if(idx>labels.size())
					throw new AuthoritiesDownloaderException("\n                                                 More idx (" + idx + ") than in labels: " + labels.toString());

				String joinedLabels = "";
				if(idx>=0)
				{
					ArrayList<String> cleanedLabels = new ArrayList<String>(labels);
					while(cleanedLabels.size()>idx+1)
						cleanedLabels.remove(0);
					for(int k=0; k < cleanedLabels.size(); k++)
					{
						String curLabel = cleanedLabels.get(k);
						if(!curLabel.isEmpty())
						{
							curLabel = curLabel.substring(0, 1).toUpperCase() + curLabel.substring(1).toLowerCase();
							joinedLabels += (" > " + curLabel);
						}
					}
					joinedLabels = THEME_SEPARATOR + joinedLabels.substring(3);
				}
				String lf_theme = references.get("name_theme");
				if(!lf_theme.isEmpty())
					lf_theme += joinedLabels;
				lf_themes.add(lf_theme);
			}
		}

		if(lf_themes.size()<=0)
			return "";
		String result = "";
		for(String lf_theme : lf_themes)
		{
			result+=(";" + lf_theme);
		}
		return result.substring(1);
	}

	// You don't understand? Me too :'(
	public void transform(int collection_id, RFHarvesterUploaderInterface volumesInterface, HashMap<String, String> dateEndNew, HashMap<String, String> authorityindices, HashMap<String, String> documenttypes, HashMap<String, String> primarydocumenttypes, HashSet<Integer> refsourcesLengthsReferences, HashMap<String, String> cduThemesReferences, HashSet<Integer> refsourcesLengthsExclusions, HashMap<String, String> cduThemesExclusions, HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesReferences, HashMap<String, HashMap<String, HashMap<String, String>>> bdmThemesExclusions) throws AuthoritiesDownloaderException
	{
		this.put("dc_title", normalizeTitle(this.get("dc_title")));
		this.put("title", this.get("dc_title"));
		this.put("dc_creator", normalizePortFolio(this.get("dc_creator")));
		this.put("dc_subject", normalizePortFolio(this.get("dc_subject")));
		this.put("dc_description", normalizePortFolio(this.get("dc_description")));
		this.put("dc_publisher", normalizePortFolio(this.get("dc_publisher")));
		this.put("publisher", this.get("dc_publisher"));
		this.put("dc_language", normalizePortFolio(this.get("dc_language_long")));
		this.put("issn", normalizePortFolio(this.get("bpi_issn")));
		this.put("isbn", normalizePortFolio(this.get("bpi_isbn")));
		this.put("dc_contributor", normalizePortFolio(this.get("dc_contributor")));
		this.put("dc_relation", normalizePortFolio(this.get("dc_relation")));
		this.put("last_issue", this.get("bpi_dernier_no"));
		this.put("issues", this.get("bpi_collection"));
		this.put("binding" /* of Isaac */, this.get("bpi_reliure"));
		this.put("dcdate", this.get("dc_date_long"));
		this.put("dc_date", this.get("dc_date_long"));
		this.put("issue_title", this.get("dc_title_revue"));
		this.put("conservation", this.get("bpi_conservation"));
		this.put("label_indice", normalizePortFolio(getCDU(authorityindices, this.get("bpi_indice")))); //Don't remove bpi_indice
		this.put("publisher_country", normalizePortFolio(this.remove("bpi_publishercountry")));
		this.put("abstract", normalizePortFolio(this.remove("bpi_abstract")));
		this.put("indice", normalizePortFolio(this.get("bpi_indice")));

		//Treatement for musical documents
		HashSet<String> dcAudioTypes = new HashSet<String>();
		dcAudioTypes.add("EMUSICAL");
		dcAudioTypes.add("CDLIV");
		dcAudioTypes.add("EPARLE");
		dcAudioTypes.add("LIVREAUDIO");
//		System.out.println("dc_format : " + this.get("dc_format"));
		if(dcAudioTypes.contains(this.get("dc_type").toUpperCase()))
		{
//			System.out.println("aa");
			this.put("commercial_number", this.get("bpi_numeroscommerciaux"));
			if(this.get("dc_type").compareToIgnoreCase("LIVREAUDIO") != 0)
			{
				this.put("musical_kind", this.get("bpi_genre_mus").replaceAll("Musiques > ", ""));
				this.put("dc_format", dcformater(normalizePortFolio(this.get("dc_format"))).replaceAll(" ; En ligne", ""));
			}
			else
			{
				this.put("musical_kind", "");
				this.put("dc_format", dcformater(normalizePortFolio(this.get("dc_format"))));
			}
		}
		else
		{
//			System.out.println("bb");
			this.put("commercial_number", "");
			this.put("musical_kind", "");
			this.put("dc_format", dcformater(normalizePortFolio(this.get("dc_format"))));
		}
//		System.out.println("dc_format : " + this.get("dc_format"));

		// Boost for revues
		HashSet<String> dcRevuesTypes = new HashSet<String>();
		dcRevuesTypes.add("REVUE");
		dcRevuesTypes.add("REVUELEC");
		int revueBoost=0;
		if(dcRevuesTypes.contains(this.get("dc_type").toUpperCase()))
			revueBoost=7000;

		String type = save_document_type(this.get("dc_type"), documenttypes, primarydocumenttypes);

		if(this.get("dc_contributor").length() > 0)
		{
			this.put("dc_contributor", this.get("dc_contributor") + ";" + normalizePortFolio(this.get("bpi_creator2")));
		}
		else
		{
			this.put("dc_contributor", normalizePortFolio(this.get("bpi_creator2")));
		}

		String coverage_spatial = normalizePortFolio(this.get("dc_coverage_spatial")).replaceAll(" @;@ ", ";");
		String coverage_temporal = normalizePortFolio(this.get("dc_coverage_temporal")).replaceAll(" @;@ ", ";");
		String[] coverage_spatial_index = coverage_spatial.split(";");
		if(coverage_spatial.length() > 0)
		{
			if(coverage_temporal.length() > 0)
			{
				this.put("dc_coverage", coverage_spatial + ";" + coverage_temporal);
			}
			else
				this.put("dc_coverage", coverage_spatial);
		}
		else if(coverage_temporal.length() > 0)
		{
			this.put("dc_coverage", coverage_temporal);
		}
		else
			this.put("dc_coverage", "");

		if(this.get("bpi_copy").length() > 0)
		{
			this.put("dc_rights", this.get("bpi_copy") + ".\n " + this.get("dc_rights"));
		}

		String document_date = this.get("bpi_nouveaute");

		String parse = "";
		try
		{
			if(this.get("bpi_dispo").startsWith("D"))
			{
				String new_period = dateEndNew.get(this.get(type));
				if(new_period.isEmpty())
				{
					this.put("solr_date_end_new", null);
				}
				else
				{
			        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			        
			        parse = "formatter.parse(" + document_date + ")";
			        Date dateStr = formatter.parse(document_date);
			        parse = "formatter.parse(" + new_period + ")";
			        String date_end_new = "" + (dateStr.getTime() + Integer.parseInt(new_period));
			        date_end_new += "T23:59:59Z";
					this.put("solr_date_end_new", date_end_new);
				}
			}
			else
			{
				this.put("solr_date_end_new", null);
			}
		}
		catch(ParseException e)
		{
			RFHarvesterLogger.warning("Unable to parse data: " + parse + "\n                                                 " + e.getMessage());
			this.put("solr_date_end_new", null);
		}
		catch(Exception e)
		{
			this.put("solr_date_end_new", null);
		}

		ArrayList<String> cotes = new ArrayList<String>();
		if(this.get("bpi_cote").length() > 0)
		{
			for(String S : this.get("bpi_cote").split(" @;@ ")) //Don't remove bpi_cote
			{
				cotes.add(S);
			}
			String cotesToString = cotes.toString();
			this.put("cote", cotesToString.substring(1, cotesToString.length()-1));
		}
		else
			this.put("cote", "");

		//Retrieve infos for notice multimedia objects (one dm_launch = one object
		ArrayList<String> bdm_info = new ArrayList<String>();
		ArrayList<String> bdms = new ArrayList<String>();
		ArrayList<String> vol_title = new ArrayList<String>();
		if(!this.get("bpi_dm_title").isEmpty())
			for(String S : this.remove("bpi_dm_title").split(" @;@ "))
			{
				vol_title.add(S);
			}
		this.put("vol_title", vol_title.toString());
		this.put("dm_launch", this.get("bpi_dm_launch")); //Don't remove bpi_dm_launch
		String barcode_field = "";

		String theme = "";
		if(this.get("dm_launch") == null || this.get("dm_launch").length()<=0) //CDU
		{
			theme = this.get("bpi_theme_lib");
			if(theme.isEmpty() && (!this.get("bpi_indice").isEmpty()))
			{
				try
				{
					theme = this.translateCduThemes(this.get("bpi_indice").replaceAll(";", "@;@"), authorityindices, refsourcesLengthsReferences, cduThemesReferences, refsourcesLengthsExclusions, cduThemesExclusions);
				}
				catch(AuthoritiesDownloaderException e)
				{
					//throw new RFHarvesterStorageClassException("Can't translateCduThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("bpi_indice") + e.getMessage());
					RFHarvesterLogger.warning("Can't translateCduThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("bpi_indice") + e.getMessage()); //TODO
					theme = "";
				}
			}
		}
		else
		{
			int i = 0;
			for(String dm : this.get("dm_launch").split(" @;@ "))
			{
				Pattern pattern = Pattern.compile("http:\\/\\/.*\\/([a-z]+)\\/LaunchOMM\\.aspx\\?IdDoc=(\\d+)&IdOmm=(\\d+)(&IdWeb=(\\d+))?");
				Matcher matcher = pattern.matcher(dm);
				String bdm = "";
				String iddoc = "";
				String idomm = "";
				String idweb = "";
				if(matcher.matches())
				{
					if(matcher.group(1) != null)
						bdm = matcher.group(1);
					if(matcher.group(2) != null)
						iddoc = matcher.group(2);
					if(matcher.group(3) != null)
						idomm = matcher.group(3);
					if(matcher.group(5) != null)
						idweb = matcher.group(5);
				}
				bdms.add(bdm);

				String doc_title = "";
				if(!vol_title.isEmpty() && vol_title.size() > i)
				{
					doc_title = vol_title.get(i);
					doc_title = (doc_title.length() > 0) ? doc_title : "";
				}

				String barcode = this.get("bpi_dm_barcode");
				if(barcode == null)
					barcode = "";
				else
				{
					ArrayList<String> barecodes = new ArrayList<String>();
					for(String S : barcode.split(" @;@ "))
						barecodes.add(S);
					if(barecodes.size() > i)
					{
						barcode = barecodes.get(i);
					}
					barcode_field = barecodes.toString().substring(1, barecodes.toString().length()-1);
				}
				bdm_info.add("bdm=" + bdm + "|iddoc=" + iddoc + "|idomm=" + idomm + "|doc_title=" + doc_title + "|barcode=" + barcode + ((idweb.length() <= 0) ? "" : ("|idweb=" + idweb)));
				i++;
			}


			String bdm = bdms.get(0);
			if((!this.get("bpi_theme").isEmpty())&&(!this.get("bpi_theme_lib").isEmpty())) //BDM
			{
				try
				{
					theme = this.translateBdmThemes(this.get("bpi_theme"), this.get("bpi_theme_lib"), bdm, bdmThemesReferences, bdmThemesExclusions);
					if(theme.isEmpty())
						throw new AuthoritiesDownloaderException("EMPTY FINAL THEME");
				}
				catch(AuthoritiesDownloaderException e)
				{
//					throw new RFHarvesterStorageClassException("Can't translateBdmThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("bpi_theme") + " ~ " + this.get("bpi_theme_lib") + e.getMessage());
					RFHarvesterLogger.warning("Can't translateBdmThemes for " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("bpi_theme") + " ~ " + this.get("bpi_theme_lib") + e.getMessage()); //TODO
				}
			}
		}
		this.put("theme", theme);
		

		String dcType = this.get("dc_type"); 
		if(dcType.compareTo("FILM") == 0 || dcType.compareTo("FILMANIM") == 0 || dcType.compareTo("SPECTAFI") == 0 || dcType.compareTo("FILMAUTO") == 0)
		{
			ArrayList<String> err = new ArrayList<String>();
			err.add("\"" + this.get("dc_identifier") + "\"");
			err.add("\"" + this.get("dc_type") + "\"");
			err.add("\"" + this.get("title") + "\"");
			err.add("\"" + document_date + "\"");
			err.add("\"" + this.get("bpi_dispo_ex") + "\"");
			err.add("\"" + this.get("bpi_dm_launch") + "\"");
			if(this.get("bpi_dm_launch").length()<=0)
			{
				throw new AuthoritiesDownloaderException("Skipped bmd_info uniformisation : Missing bpi_dm_launch: " + err.toString());
			}
			else
			{
				int bpiDispoExLength = this.get("bpi_dispo_ex").split("@;@").length;
				int bpiDmLaunchLength = this.get("bpi_dm_launch").split("@;@").length;

				if(bpiDispoExLength == (bpiDmLaunchLength * 2))
				{
					if(dcType.compareTo("FILM") == 0 || dcType.compareTo("FILMANIM") == 0 || dcType.compareTo("SPECTAFI") == 0)
					{
						int bdmInfoSize = bdm_info.size();
						for(int i = 0; i < bdmInfoSize; i++)
						{
							bdm_info.add(bdm_info.get(i));
						}
					}
					else
						throw new AuthoritiesDownloaderException("Skipped bmd_info uniformisation : Invalid count ("+bpiDispoExLength+" bpi_dispo_ex, "+bpiDmLaunchLength +" bpi_dm_launch): " + err.toString());
				}
				else if(bpiDispoExLength == bpiDmLaunchLength)
				{
					int bdmInfoSize = bdm_info.size();
					for(int i = 0; i < bdmInfoSize; i++)
					{
						bdm_info.add(bdm_info.get(i));
					}
				}
				else
					throw new AuthoritiesDownloaderException("Skipped bmd_info uniformisation : Invalid count ("+bpiDispoExLength+" bpi_dispo_ex, "+bpiDmLaunchLength +" bpi_dm_launch): " + err.toString());		
			}
		}

		try
		{
			if(this.get("bpi_dispo").startsWith("D"))
			{
				this.put("is_available", "1");
			}
			else
			{
				this.put("is_available", "0");
			}
		}
		catch(Exception e)
		{
			this.put("is_available", null);
		}

		this.put("musical_kind_keyword", this.get("musical_kind"));
		this.put("barcode_field_keyword", barcode_field);
		String keyword="";
		keyword += (this.get("title") != null)? " " + this.get("title") : "";
		keyword += (this.get("dc_creator") != null)? " " + this.get("dc_creator") : "";
		keyword += (this.get("dc_contributor") != null)? " " + this.get("dc_contributor") : "";
		keyword += (this.get("dc_subject") != null)? " " + this.get("dc_subject") : "";
		keyword += (this.get("dc_description") != null)? " " + this.get("dc_description") : "";
		keyword += (this.get("publisher") != null)? " " + this.get("publisher") : "";
		keyword += (theme != null)? " " + theme : "";
		keyword += (type != null)? " " + type : "";
		keyword += (this.get("dc_relation") != null)? " " + this.get("dc_relation") : "";
		keyword += (this.get("musical_kind") != null)? " " + this.get("musical_kind") : "";
		keyword += (this.get("barcode_field_keyword") != null)? " " + this.get("barcode_field_keyword") : "";
		keyword += (this.get("abstract") != null)? " " + this.get("abstract") : "";

		this.put("keyword", normalizePortFolio(keyword));

		String broadCastGroup = this.get("bpi_gr_diff");
		this.put("broadcast_group", (broadCastGroup == null || broadCastGroup.isEmpty()) ? "" : broadCastGroup.replaceAll(" @;@ ", ";"));
		
		//For controls statement
		this.put("oai_identifier", this.get("dc_identifier"));
		// title done
		this.put("collection_id", ""+collection_id);
		this.put("description", this.get("dc_description"));
		this.put("collection_name", "portfoliodw");
		this.put("url", null);

		//For metadatas statement
		// collection_id done
		// dc_title done
		// dc_creator done
		// dc_subject done
		// dc_description done
		// dc_publisher done
		// dc_contributor done
		// dc_date done
		this.put("dc_type", type);
		// dc_format done
		// dc_identifier done
		// dc_relation done
		// dc_coverage done
		// dc_rights done
		// dc_language done
		this.put("dc_coverage_spatial", coverage_spatial);
		this.put("controls_id", null);
		this.put("dc_identifier_text", null);
		this.put("dc_source", null);
		this.put("osu_volume", null);
		this.put("osu_issue", null);
		this.put("osu_linking", null);
		this.put("osu_openurl", null);
		this.put("osu_thumbnail", "");

		//For portfolio_datas statement
		// dc_identifier done
		this.put("audience", this.get("bpi_audience"));
		this.put("genre", (this.get("bpi_genre") == null) ? "" : this.get("bpi_genre"));
		// last_issue done
		// issn done
		// isbn done
		// theme done
		// is_available done
		// indice done
		// label_indice done
		// broadcast_group done
		// issues done
		// binding done
		// issue_title done
		// conservation done
		// commercial_number done
		// musical_kind done
		// publisher_country done
		// abstract done
		this.put("call_num", null);
		this.put("copyright", null);
		this.put("display_group", null);
		this.put("license_info", null);
		this.put("metadata_id", null);
		this.put("display_groups", null);
		

		//For volumes statement
		// dc_identifier done
		// collection_id done
		// * availability
		// * call_num
		// * location
		// * label
		// * link
		// * launch_url
		// * support
		// * number
		// * barcode
		// * document_id
		// * object_id
		// * source
		// * launchable
		// * note
		// * external_access
		this.put("link_label", null);
		//this.put("metadata_id", null); done

//		System.out.println("dc_type : " + this.get("dc_type"));
		String[] note = (this.get("dc_type").compareToIgnoreCase("CARTE") != 0 && !this.get("bpi_notes_ex").isEmpty()) ? this.get("bpi_notes_ex").split(" @;@ ") : null;

		String[] call_num = (!this.get("bpi_cote").isEmpty()) ? this.get("bpi_cote").split(" @;@ ") : null;
		String[] location = (!this.get("bpi_loca").isEmpty()) ? this.get("bpi_loca").split(" @;@ ") : null;
		String[] label = (!this.get("bpi_dm_lien_lib").isEmpty()) ? this.get("bpi_dm_lien_lib").split(" @;@ ") : null;
		String[] launch_url = (!this.get("bpi_dm_launch").isEmpty()) ? this.get("bpi_dm_launch").split(" @;@ ") : null;
		String[] support = (!this.get("dc_format_court").isEmpty()) ? this.get("dc_format_court").split(" @;@ ") : null;
		String[] resource_link = (!this.get("bpi_dm_lien_url").isEmpty()) ? this.get("bpi_dm_lien_url").split(" @;@ ") : null;
		String[] launchable = (!this.get("bpi_dm_type").isEmpty()) ? this.get("bpi_dm_type").split(" @;@ ") : null;
		String[] external_access = (!this.get("bpi_dm_access").isEmpty()) ? this.get("bpi_dm_access").split(" @;@ ") : null;

		String dm = this.get("dm_launch");
		Pattern patternLeft = Pattern.compile("IdOmm.*@;@");
		Pattern patternRight = Pattern.compile(".*@;@.*IdOmm");
		Matcher matcherLeft = patternLeft.matcher(dm);
		Matcher matcherRight = patternRight.matcher(dm);
		boolean matcheLeft = matcherLeft.find();
		boolean matcheRight = matcherRight.find();
		int idx = 0;

		ArrayList<HashMap<String, String>> volumes = new ArrayList<HashMap<String, String>>();

		Pattern patternBarCode = Pattern.compile("barcode=(\\d+)");
		Pattern patternDocId = Pattern.compile("iddoc=(\\d+)");
		Pattern patternObjectId = Pattern.compile("idomm=(\\d+)");
		Pattern patternSource = Pattern.compile("bdm=([a-z]+)", Pattern.CASE_INSENSITIVE);

		//TODO Set note, link, launch_url, support to null instead of ""
		//TODO Set launchable and external_access to null instead of 0 in first test
		//TODO Kill the guy who forced me to write this shit!
		if(!this.get("bpi_dispo_ex").isEmpty() && (!matcheLeft || !matcheRight))
		{
//			System.out.println("aa");
			for(String volume : this.get("bpi_dispo_ex").split(" @;@ "))
			{
				if(note != null && note.length > 0)
				{
					if(idx < note.length)
						this.put("note", note[idx].trim());
				}
				else
					this.put("note", "");

				if(call_num != null && call_num.length > 0)
				{
					if(idx < call_num.length)
						this.put("call_num", call_num[idx].trim());
				}
				else
					this.put("call_num", null);

				if(location != null && location.length > 0)
				{
					if(idx < location.length)
						this.put("location", location[idx].trim());
				}
				else
					this.put("location", null);

				this.put("availability", volume.trim());

				if(vol_title.isEmpty() || idx >= vol_title.size())
				{
					if(label == null || idx >= label.length)
						this.put("label", this.get("title"));
					else
						this.put("label", label[idx]);
				}
				else if(idx < vol_title.size())
				{
					this.put("label", vol_title.get(idx));
				}
				else
				{
					this.put("label", this.get("title"));
				}

				if(resource_link != null && resource_link.length > 0)
				{
					if(idx < resource_link.length)
						this.put("link", resource_link[idx].trim());
				}
				else
					this.put("link", "");

				if(launch_url != null && launch_url.length > 0)
				{
					if(idx < launch_url.length)
						this.put("launch_url", launch_url[idx].trim());
				}
				else
					this.put("launch_url", "");

				if(support != null && support.length > 0)
				{
					if(idx < support.length)
						this.put("support", support[idx].trim());
				}
				else
					this.put("support", "");

				this.put("number", ""+(idx+1));

				this.put("barcode", "");
				this.put("document_id", "0");
				this.put("object_id", "0");
				this.put("source", "");
//				System.out.println(bdm_info);
				if(!bdm_info.isEmpty() &&  idx < bdm_info.size())
				{
//					System.out.println("aaa");
					Matcher matcherBarCode = patternBarCode.matcher(bdm_info.get(idx));
					Matcher matcherDocId = patternDocId.matcher(bdm_info.get(idx));
					Matcher matcherObjectId = patternObjectId.matcher(bdm_info.get(idx));
					Matcher matcherSource = patternSource.matcher(bdm_info.get(idx));
					if(matcherBarCode.find())
						this.put("barcode", matcherBarCode.group(1));
					if(matcherDocId.find())
						this.put("document_id", matcherDocId.group(1));
					if(matcherObjectId.find())
						this.put("object_id", matcherObjectId.group(1));
					if(matcherSource.find())
						this.put("source", matcherSource.group(1));
				}

				if(launchable != null && launchable.length > 0)
				{
					if(idx < launchable.length)
						this.put("launchable", ((launchable[idx]).compareTo("0")==0)? "0" : "1");
				}
				else
					this.put("launchable", "0");

				if(external_access != null && external_access.length > 0)
				{
					if(idx < external_access.length)
						this.put("external_access", ((external_access[idx]).compareTo("f")==0)? "0" : "1");
				}
				else
					this.put("external_access", "0");

				HashMap<String, String> volume_element = new HashMap<String, String>();
				volume_element.put("dispo", volume.trim());
				volume_element.put("link", this.get("link"));
				volume_element.put("format", this.get("dc_format"));
				volumes.add(volume_element);

				try
				{
					if(volumesInterface!=null)
						volumesInterface.insertRow(this);
				}
				catch(RFHarvesterUploaderException e)
				{
					RFHarvesterLogger.error(e.getMessage());
					e.printStackTrace();
				}
				idx++;
			}
		}
		else if(!this.get("bpi_cote").isEmpty() && (!matcheLeft || !matcheRight)) //TODO merge this condition with previous (almost same thing)
		{
			for(String cote : this.get("bpi_cote").split(" @;@ "))
			{
				if(note != null && note.length > 0)
				{
					if(idx < note.length)
						this.put("note", note[idx].trim());
				}
				else
					this.put("note", "");
				this.put("call_num", cote);
				if(location != null && location.length > 0)
				{
					if(idx < location.length)
						this.put("location", location[idx].trim());
				}
				else
					this.put("location", null);
				this.put("availability", "Disponible");

				if(vol_title.isEmpty() || idx > vol_title.size())
				{
					if(label == null || idx >= label.length)
						this.put("label", this.get("title"));
					else
						this.put("label", label[idx]);
				}
				else if(idx < vol_title.size())
				{
					this.put("label", vol_title.get(idx));
				}
				else
				{
					this.put("label", this.get("title"));
				}

				if(resource_link != null && resource_link.length > 0)
				{
					if(idx < resource_link.length)
						this.put("link", resource_link[idx].trim());
				}
				else
					this.put("link", "");

				if(launch_url != null && launch_url.length > 0)
				{
					if(idx < launch_url.length)
						this.put("launch_url", launch_url[idx].trim());
				}
				else
					this.put("launch_url", "");

				if(support != null && support.length > 0)
				{
					if(idx < support.length)
						this.put("support", support[idx].trim());
				}
				else
					this.put("support", "");

				this.put("number", ""+(idx+1));

				this.put("barcode", "");
				this.put("document_id", "0");
				this.put("object_id", "0");
				this.put("source", "");

				if(launchable != null && launchable.length > 0)
				{
					if(idx < launchable.length)
						this.put("launchable", ((launchable[idx]).compareTo("0")==0)? "0" : "1");
				}
				else
					this.put("launchable", "0");

				if(external_access != null && external_access.length > 0)
				{
					if(idx < external_access.length)
						this.put("external_access", ((external_access[idx]).compareTo("f")==0)? "0" : "1");
				}
				else
					this.put("external_access", "0");

				HashMap<String, String> volume_element = new HashMap<String, String>();
				volume_element.put("link", this.get("link"));
				volume_element.put("dispo", "Disponible");
				volume_element.put("format", this.get("dc_format"));
				volumes.add(volume_element);

				try
				{
					if(volumesInterface!=null)
						volumesInterface.insertRow(this);
				}
				catch(RFHarvesterUploaderException e)
				{
					RFHarvesterLogger.error(e.getMessage());
					e.printStackTrace();
				}
				idx++;
			}
		}
		else if(matcheLeft || matcheRight)
		{
//			System.out.println("cc");
			String[] exemplaires = this.get("bpi_dm_launch").split(" @;@ ");
			for(int i =0; i < exemplaires.length; i++)
			{
				if(note != null && note.length > 0)
				{
					if(idx < note.length)
						this.put("note", note[idx].trim());
				}
				else
					this.put("note", "");

				if(call_num != null && call_num.length > 0)
				{
					if(idx < call_num.length)
						this.put("call_num", call_num[idx].trim());
				}
				else
					this.put("call_num", null);

				if(location != null && location.length > 0)
				{
					if(idx < location.length)
						this.put("location", location[idx].trim());
				}
				else
					this.put("location", null);

				this.put("availability", "Consultable sur ce poste");
				if(!vol_title.isEmpty() && idx < vol_title.size())
					this.put("label", vol_title.get(idx));
				else
					this.put("label", (exemplaires.length >1) ? "Lancer le document" : this.get("title"));

				if(resource_link != null && resource_link.length > 0)
				{
					if(idx < resource_link.length)
						this.put("link", resource_link[idx].trim());
				}
				else
					this.put("link", "");

				if(launch_url != null && launch_url.length > 0)
				{
					if(idx < launch_url.length)
						this.put("launch_url", launch_url[idx].trim());
				}
				else
					this.put("launch_url", "");

				if(support != null && support.length > 0)
				{
					if(idx < support.length)
						this.put("support", support[idx].trim());
				}
				else
					this.put("support", "");

				this.put("number", "" + (idx+1));
				

				this.put("barcode", "");
				this.put("document_id", "0");
				this.put("object_id", "0");
				this.put("source", "");
				if(!bdm_info.isEmpty() &&  idx < bdm_info.size())
				{
					Matcher matcherBarCode = patternBarCode.matcher(bdm_info.get(idx));
					Matcher matcherDocId = patternDocId.matcher(bdm_info.get(idx));
					Matcher matcherObjectId = patternObjectId.matcher(bdm_info.get(idx));
					Matcher matcherSource = patternSource.matcher(bdm_info.get(idx));
					if(matcherBarCode.find())
						this.put("barcode", matcherBarCode.group(1));
					if(matcherDocId.find())
						this.put("document_id", matcherDocId.group(1));
					if(matcherObjectId.find())
						this.put("object_id", matcherObjectId.group(1));
					if(matcherSource.find())
						this.put("source", matcherSource.group(1));
				}

				if(launchable != null && launchable.length > 0)
				{
					if(idx < launchable.length)
						this.put("launchable", ((launchable[idx]).compareTo("0")==0)? "0" : "1");
				}
				else
					this.put("launchable", "0");

				if(external_access != null && external_access.length > 0)
				{
					if(idx < external_access.length)
						this.put("external_access", ((external_access[idx]).compareTo("f")==0)? "0" : "1");
				}
				else
					this.put("external_access", "0");

				HashMap<String, String> volume_element = new HashMap<String, String>();
				volume_element.put("link", this.get("link"));
				volume_element.put("dispo", "Consultable sur ce poste");
				volume_element.put("format", this.get("dc_format"));
				volumes.add(volume_element);

				try
				{
					if(volumesInterface!=null)
						volumesInterface.insertRow(this);
				}
				catch(RFHarvesterUploaderException e)
				{
					RFHarvesterLogger.error(e.getMessage());
					e.printStackTrace();
				}
				idx++;
			}
//			throw new RFHarvesterStorageClassException("Found a last volume match: " + this.get("dc_identifier") + " ~ " + this.get("title") + " ~ " + this.get("dm_launch"));
		}

		SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");
		String harvesting_date = (document_date!=null && document_date.length()>0)? document_date : formater.format(new Date());


		String dispo_sur_poste = "";
		String dispo_bibliotheque = "";
		String dispo_access_libre = "";
		String dispo_avec_reservation = "";
		String dispo_broadcast_group = this.get("broadcast_group");

		Pattern patternOnShelf = Pattern.compile("(papier|microforme|microfilm|imprimé)", Pattern.CASE_INSENSITIVE);
		Pattern patternOnLine = Pattern.compile("(en ligne|DVD|CD-ROM|\\sCD\\s|cédérom|internet|vidéo)", Pattern.CASE_INSENSITIVE);
		Pattern patternDispo = Pattern.compile("(disponible|bureau)", Pattern.CASE_INSENSITIVE);
		for(HashMap<String, String> volume : volumes)
		{
			String volumeDispo=volume.get("dispo");
			String volumeFormat=volume.get("format");
			Matcher matcherOnShelf = patternOnShelf.matcher(volumeFormat);
			Matcher matcherOnLine = patternOnLine.matcher(volumeFormat);
			Matcher matcherDispo = patternDispo.matcher(volumeDispo);

			matcherOnShelf.reset();
			matcherOnLine.reset();
			matcherDispo.reset();
			if((resource_link != null)&&(matcherOnLine.find()))
			{
				dispo_sur_poste = "online";
				dispo_bibliotheque = "online";
				dispo_access_libre = "online";
				dispo_avec_reservation = "online";
				break;
			}
			else if(matcherDispo.find() && (matcherOnShelf.find()))// || matcherOnLine.find()))
			{
				dispo_sur_poste = "onshelf";
				dispo_bibliotheque = "onshelf";
				dispo_access_libre = "onshelf";
				dispo_avec_reservation = "onshelf";
			}
		}


		
		//For SOLR statement
		this.put("solr_id", this.get("dc_identifier") + ";" + collection_id);
		this.put("solr_collection_id", "" + collection_id);
		this.put("solr_controls_id", this.get("dc_identifier"));
		this.put("solr_collection_name", "portfoliodw");
		this.put("solr_title", this.get("title") + " " + this.get("dc_relation"));
		this.put("solr_creator", this.get("dc_creator") + "; " + normalizePortFolio(this.get("bpi_creator2")));
		this.put("solr_subject", this.get("dc_subject"));
		this.put("solr_description", this.get("dc_description"));
		this.put("solr_publisher", this.get("publisher"));
		this.put("solr_keyword", this.get("keyword"));
//		System.out.println("theme : " + this.get("theme"));
		this.put("solr_theme", this.get("theme"));
		this.put("solr_theme_rebond", this.get("theme"));
		this.put("solr_document_type", type);
		if(harvesting_date == null || harvesting_date.length()<=0)
			this.put("solr_harvesting_date", null);
		else if(harvesting_date.length()==4)
			this.put("solr_harvesting_date", harvesting_date+"-01-01T23:59:59Z");
		else if(harvesting_date.length()==6)
			this.put("solr_harvesting_date", harvesting_date.substring(0,4)+"-"+harvesting_date.substring(4,6)+"-01T23:59:59Z");
		else if(harvesting_date.length()==8)
			this.put("solr_harvesting_date", harvesting_date.substring(0,4)+"-"+harvesting_date.substring(4,6)+"-"+harvesting_date.substring(6,8)+"T23:59:59Z");
		else
			this.put("solr_harvesting_date", "1000-01-01T23:59:59Z");

//		this.put("solr_harvesting_date", harvesting_date+"T23:59:59Z");
		this.put("solr_isbn", this.get("isbn"));
		this.put("solr_issn", this.get("issn"));
		this.put("solr_cote_rebond", this.get("cote"));
		String bdmInfo = bdm_info.toString();
		bdmInfo = bdmInfo.substring(1, bdmInfo.length()-1);
		this.put("solr_bdm_info", bdmInfo);
		this.put("solr_autocomplete", this.get("keyword"));
		this.put("solr_autocomplete_creator", this.get("solr_creator"));
		this.put("solr_autocomplete_publisher", this.get("publisher"));
		this.put("solr_autocomplete_theme", this.get("theme"));
		this.put("solr_autocomplete_title", this.get("solr_title"));
		this.put("solr_autocomplete_subject", this.get("dc_subject"));
		this.put("solr_autocomplete_description", this.get("dc_description"));
		this.put("solr_barcode", barcode_field);
		this.put("solr_indice", this.get("bpi_indice"));
		this.put("solr_custom_document_type", this.get("dc_type"));
		this.put("solr_title_sort", this.get("title"));
		this.put("solr_lang_exact", this.get("dc_language"));
		this.put("dcdate", this.get("dcdate").replaceAll("[^\\d]", ""));
		if(this.get("dcdate") == null || this.get("dcdate").length()<=0)
			this.put("solr_date_document", null);
		else if(this.get("dcdate").length()==4)
			this.put("solr_date_document", this.get("dcdate")+"-01-01T01:00:00Z");
		else if(this.get("dcdate").length()==6)
		{
			int month = Integer.parseInt(this.get("dcdate").substring(4,6));
			if(month < 1 || month > 12)
				this.put("solr_date_document", "1000-01-01T01:00:00Z");
			else
				this.put("solr_date_document", this.get("dcdate").substring(0,4)+"-"+this.get("dcdate").substring(4,6)+"-01T01:00:00Z");
		}
		else if(this.get("dcdate").length()==8)
		{
			int month = Integer.parseInt(this.get("dcdate").substring(4,6));
			int day = Integer.parseInt(this.get("dcdate").substring(6,8));
			if(month < 1 || month > 12)
				this.put("solr_date_document", "1000-01-01T01:00:00Z");
			else if (day < 1 || day > 31)
				this.put("solr_date_document", this.get("dcdate").substring(0,4)+"-"+this.get("dcdate").substring(4,6)+"-01T01:00:00Z");
			else
				this.put("solr_date_document", this.get("dcdate").substring(0,4)+"-"+this.get("dcdate").substring(4,6)+"-"+this.get("dcdate").substring(6,8)+"T01:00:00Z");
		}
		else
			this.put("solr_date_document", "1000-01-01T01:00:00Z");
		this.put("solr_dispo_sur_poste", dispo_sur_poste);
		this.put("solr_dispo_bibliotheque", dispo_bibliotheque);
		this.put("solr_dispo_access_libre", dispo_access_libre);
		this.put("solr_boost", "" + (5000 + revueBoost));
//		this.put("solr_boost", "" + (5000));
		this.put("solr_dispo_avec_reservation", dispo_avec_reservation);
		this.put("solr_dispo_broadcast_group", dispo_broadcast_group);
		this.put("solr_rights", this.get("dc_rights"));
		/*String locationString = Arrays.toString(location);
		locationString = locationString.substring(1, locationString.length()-1);*/
		String locationString = "";
		for(String loc : location)
			locationString+=("@"+loc+"@;");
		locationString = locationString.substring(1, locationString.length()-2);
//		System.out.println("solr_location : " +  locationString);
		this.put("solr_location", locationString);
		String coverageSpatialIndexString = (coverage_spatial_index.length > 0) ? coverage_spatial_index[0] : "";
		for(int i=1; i < coverage_spatial_index.length; i++)
			coverageSpatialIndexString+=coverage_spatial_index[i];
		this.put("solr_coverage_spatial", coverageSpatialIndexString);
		// solr_date_end_new done
		this.put("solr_commercial_number", this.get("commercial_number"));
		this.put("solr_musical_kind", this.get("musical_kind"));
	}
}

// U mad bro?
//
//                                       .....'',;;::cccllllllllllllcccc:::;;,,,''...'',,'..
//                            ..';cldkO00KXNNNNXXXKK000OOkkkkkxxxxxddoooddddddxxxxkkkkOO0XXKx:.
//                      .':ok0KXXXNXK0kxolc:;;,,,,,,,,,,,;;,,,''''''',,''..              .'lOXKd'
//                 .,lx00Oxl:,'............''''''...................    ...,;;'.             .oKXd.
//              .ckKKkc'...'',:::;,'.........'',;;::::;,'..........'',;;;,'.. .';;'.           'kNKc.
//           .:kXXk:.    ..       ..................          .............,:c:'...;:'.         .dNNx.
//          :0NKd,          .....''',,,,''..               ',...........',,,'',,::,...,,.        .dNNx.
//         .xXd.         .:;'..         ..,'             .;,.               ...,,'';;'. ...       .oNNo
//         .0K.         .;.              ;'              ';                      .'...'.           .oXX:
//        .oNO.         .                 ,.              .     ..',::ccc:;,..     ..                lXX:
//       .dNX:               ......       ;.                'cxOKK0OXWWWWWWWNX0kc.                    :KXd.
//     .l0N0;             ;d0KKKKKXK0ko:...              .l0X0xc,...lXWWWWWWWWKO0Kx'                   ,ONKo.
//   .lKNKl...'......'. .dXWN0kkk0NWWWWWN0o.            :KN0;.  .,cokXWWNNNNWNKkxONK: .,:c:.      .';;;;:lk0XXx;
//  :KN0l';ll:'.         .,:lodxxkO00KXNWWWX000k.       oXNx;:okKX0kdl:::;'',;coxkkd, ...'. ...'''.......',:lxKO:.
// oNNk,;c,'',.                      ...;xNNOc,.         ,d0X0xc,.     .dOd,           ..;dOKXK00000Ox:.   ..''dKO,
//'KW0,:,.,:..,oxkkkdl;'.                'KK'              ..           .dXX0o:'....,:oOXNN0d;.'. ..,lOKd.   .. ;KXl.
//;XNd,;  ;. l00kxoooxKXKx:..ld:         ;KK'                             .:dkO000000Okxl;.   c0;      :KK;   .  ;XXc
//'XXdc.  :. ..    '' 'kNNNKKKk,      .,dKNO.                                   ....       .'c0NO'      :X0.  ,.  xN0.
//.kNOc'  ,.      .00. ..''...      .l0X0d;.             'dOkxo;...                    .;okKXK0KNXx;.   .0X:  ,.  lNX'
// ,KKdl  .c,    .dNK,            .;xXWKc.                .;:coOXO,,'.......       .,lx0XXOo;...oNWNXKk:.'KX;  '   dNX.
//  :XXkc'....  .dNWXl        .';l0NXNKl.          ,lxkkkxo' .cK0.          ..;lx0XNX0xc.     ,0Nx'.','.kXo  .,  ,KNx.
//   cXXd,,;:, .oXWNNKo'    .'..  .'.'dKk;        .cooollox;.xXXl     ..,cdOKXXX00NXc.      'oKWK'     ;k:  .l. ,0Nk.
//    cXNx.  . ,KWX0NNNXOl'.           .o0Ooldk;            .:c;.':lxOKKK0xo:,.. ;XX:   .,lOXWWXd.      . .':,.lKXd.
//     lXNo    cXWWWXooNWNXKko;'..       .lk0x;       ...,:ldk0KXNNOo:,..       ,OWNOxO0KXXNWNO,        ....'l0Xk,
//     .dNK.   oNWWNo.cXK;;oOXNNXK0kxdolllllooooddxk00KKKK0kdoc:c0No        .'ckXWWWNXkc,;kNKl.          .,kXXk,
//      'KXc  .dNWWX;.xNk.  .kNO::lodxkOXWN0OkxdlcxNKl,..        oN0'..,:ox0XNWWNNWXo.  ,ONO'           .o0Xk;
//      .ONo    oNWWN0xXWK, .oNKc       .ONx.      ;X0.          .:XNKKNNWWWWNKkl;kNk. .cKXo.           .ON0;
//      .xNd   cNWWWWWWWWKOkKNXxl:,'...;0Xo'.....'lXK;...',:lxk0KNWWWWNNKOd:..   lXKclON0:            .xNk.
//      .dXd   ;XWWWWWWWWWWWWWWWWWWNNNNNWWNNNNNNNNNWWNNNNNNWWWWWNXKNNk;..        .dNWWXd.             cXO.
//      .xXo   .ONWNWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWNNK0ko:'..OXo          'l0NXx,              :KK,
//      .OXc    :XNk0NWXKNWWWWWWWWWWWWWWWWWWWWWNNNX00NNx:'..       lXKc.     'lONN0l.              .oXK:
//      .KX;    .dNKoON0;lXNkcld0NXo::cd0NNO:;,,'.. .0Xc            lXXo..'l0NNKd,.              .c0Nk,
//      :XK.     .xNX0NKc.cXXl  ;KXl    .dN0.       .0No            .xNXOKNXOo,.               .l0Xk;.
//     .dXk.      .lKWN0d::OWK;  lXXc    .OX:       .ONx.     . .,cdk0XNXOd;.   .'''....;c:'..;xKXx,
//     .0No         .:dOKNNNWNKOxkXWXo:,,;ONk;,,,,,;c0NXOxxkO0XXNXKOdc,.  ..;::,...;lol;..:xKXOl.
//     ,XX:             ..';cldxkOO0KKKXXXXXXXXXXKKKKK00Okxdol:;'..   .';::,..':llc,..'lkKXkc.
//     :NX'    .     ''            ..................             .,;:;,',;ccc;'..'lkKX0d;.
//     lNK.   .;      ,lc,.         ................        ..,,;;;;;;:::,....,lkKX0d:.
//    .oN0.    .'.      .;ccc;,'....              ....'',;;;;;;;;;;'..   .;oOXX0d:.
//    .dN0.      .;;,..       ....                ..''''''''....     .:dOKKko;.
//     lNK'         ..,;::;;,'.........................           .;d0X0kc'.
//      .xXO'                                                 .;oOK0x:.
//       .cKKo.                                    .,:oxkkkxk0K0xc'.
//         .oKKkc,.                         .';cok0XNNNX0Oxoc,.
//           .;d0XX0kdlc:;,,,',,,;;:clodkO0KK0Okdl:,'..
//               .,coxO0KXXXXXXXKK0OOxdoc:,..
//                         ...