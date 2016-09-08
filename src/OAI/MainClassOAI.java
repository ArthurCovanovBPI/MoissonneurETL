package OAI;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import rfharvester.transformator.RFHarvesterCodeTransformator;


public class MainClassOAI
{
//	private static String myXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\"> <responseDate>2016-03-07T16:13:53Z</responseDate> <request verb=\"ListRecords\" resumptionToken=\"1457367232\">http://oai.cairn.info/oai.php</request> <ListRecords>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0057</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0057</dc:identifier><dc:title>\"Nos innovations ont dÃ©jÃ  produit leurs effets\"</dc:title><dc:creator>Gordon, Robert </dc:creator><dc:creator> Chavagneux, Christian</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.57-57</dc:source><dc:coverage>1</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0063</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0063</dc:identifier><dc:title>La biodiversitÃ© est notre richesse</dc:title><dc:creator>Lapeyre, Renaud </dc:creator><dc:creator> Laurans, Yann</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.63-63</dc:source><dc:coverage>2</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0068</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0068</dc:identifier><dc:title>Patrick DrahiÂ : un empire Ã  crÃ©dit</dc:title><dc:creator>Renier, Romain</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.68-68</dc:source><dc:coverage>3</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0072</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0072</dc:identifier><dc:title>Les entreprises ont-elles vraiment besoin de dÃ©finir une stratÃ©gieÂ ?</dc:title><dc:creator>Mousli, Marc</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.72-72</dc:source><dc:coverage>4</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0074</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0074</dc:identifier><dc:title>La directive qui fÃ¢che</dc:title><dc:creator>Chevallier, Marc</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.74-74</dc:source><dc:coverage>5</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0080</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0080</dc:identifier><dc:title>La libÃ©ralisation financiÃ¨re nourrit les inÃ©galitÃ©s</dc:title><dc:creator>Chavagneux, Christian</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.80-80</dc:source><dc:coverage>6</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0076</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0076</dc:identifier><dc:title>DÃ©mocratieÂ : la fin d'un cycleÂ ?</dc:title><dc:creator>Martinache, Igor</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.76-76</dc:source><dc:coverage>7</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0082</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0082</dc:identifier><dc:title>La rÃ©volution solaire</dc:title><dc:creator>de Ravignan, Antoine</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.82-82</dc:source><dc:coverage>8</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0083</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0083</dc:identifier><dc:title>MobilitÃ© gÃ©ographique et emploi ne vont pas de pair</dc:title><dc:creator>Foulon, Sandrine</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.83-83</dc:source><dc:coverage>9</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0081</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0081</dc:identifier><dc:title>La sociologie excuse-t-elle les terroristesÂ ?</dc:title><dc:creator>Molenat, Xavier</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.81-81</dc:source><dc:coverage>10</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0092</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0092</dc:identifier><dc:title>Stanley Milgram et la science de l'obÃ©issance</dc:title><dc:creator>Martinache, Igor</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.92-92</dc:source><dc:coverage>11</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0094</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0094</dc:identifier><dc:title>L'Ã©tat d'urgence, un rÃ©gime civil d'exception</dc:title><dc:creator>Vindt, GÃ©rard</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.94-94</dc:source><dc:coverage>12</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>restricted access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:AE_353_0098</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>AE</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=AE_353_0098</dc:identifier><dc:title>Le bloc-notes de janvier 2016</dc:title><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Alternatives Ã©conomiques, N 353, 1, 2016-01-01, pp.98-98</dc:source><dc:coverage>13</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0022</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0022</dc:identifier><dc:title>ChapitreÂ II. Lâinstitutionnalisation de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.22-34</dc:source><dc:coverage>14</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0001</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0001</dc:identifier><dc:title>Page de dÃ©but</dc:title><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.1-2</dc:source><dc:coverage>15</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0007</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0007</dc:identifier><dc:title>Chapitre premier. GenÃ¨se de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.7-21</dc:source><dc:coverage>16</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0003</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0003</dc:identifier><dc:title>Introduction</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.3-6</dc:source><dc:coverage>17</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0059</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0059</dc:identifier><dc:title>ChapitreÂ IV. Prendre la mesure de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.59-77</dc:source><dc:coverage>18</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0035</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0035</dc:identifier><dc:title>ChapitreÂ III. Les fondements thÃ©oriques de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.35-58</dc:source><dc:coverage>19</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0078</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0078</dc:identifier><dc:title>ChapitreÂ V. Lâimpact financier de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.78-92</dc:source><dc:coverage>20</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0115</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0115</dc:identifier><dc:title>Conclusion</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.115-122</dc:source><dc:coverage>21</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0123</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0123</dc:identifier><dc:title>Bibliographie</dc:title><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.123-125</dc:source><dc:coverage>22</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0093</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0093</dc:identifier><dc:title>ChapitreÂ VI. Lâoutillage de la responsabilitÃ© sociale de lâentreprise</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.93-104</dc:source><dc:coverage>23</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0105</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0105</dc:identifier><dc:title>ChapitreÂ VII. Les dÃ©fis contemporains de la RSE</dc:title><dc:creator>Gond, Jean-Pascal </dc:creator><dc:creator> Igalens, Jacques</dc:creator><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.105-114</dc:source><dc:coverage>24</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <record>   <header>    <identifier>oai:cairn.info:PUF_GOND_2016_01_0126</identifier>    <datestamp>2016-03-07</datestamp>    <setSpec>QSJ</setSpec>   </header>   <metadata>     <oai_dc:dc       xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"       xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"       xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/       http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"><dc:identifier>http://www.cairn.info/article.php?ID_ARTICLE=PUF_GOND_2016_01_0126</dc:identifier><dc:title>Page de fin</dc:title><dc:date>2016</dc:date><dc:language>fre</dc:language><dc:source>Que sais-jeÂ ?, 5<sup>e</sup> Ã©d., 2016-03-07, pp.126-128</dc:source><dc:coverage>25</dc:coverage><dc:rights>Cairn</dc:rights>       <dcterms:accessRights>free access</dcterms:accessRights>     </oai_dc:dc>   </metadata>  </record>  <resumptionToken completeListSize=\"412225\"     cursor=\"412200\"></resumptionToken> </ListRecords></OAI-PMH>";
//	private static String myXML = "<resumptionToken completeListSize=\"584465\" cursor=\"0\">345459</resumptionToken>";
	public static void main(String args[])
	{
//		String oaiLink = "http://oai.bnf.fr/oai2/OAIHandler";
		String oaiLink = "http://oai.cairn.info/oai.php";
		System.out.println("Hello");
//		String link = oaiLink + "?verb=ListRecords&resumptionToken=1459439781";
		String link = oaiLink + "?verb=ListRecords&metadataPrefix=oai_dc";
		System.out.println(link);
		URL url;
		try
		{
			File resultFile = new File("transformation.txt");
			resultFile.delete();
			OutputStream resultFileOutputStream = new FileOutputStream(resultFile, true);

			int ptr = 0;
			url = new URL(link);
			StringBuffer buffer = new StringBuffer();

//			InputStream IS = url.openStream();
			URLConnection con = url.openConnection();
			InputStream IS = con.getInputStream();

			ptr = 0;
			buffer = new StringBuffer();
			while ((ptr = IS.read()) != -1)
			    buffer.append((char)ptr);

			System.out.println("Connection openned");
			//System.out.println(buffer);
			String xml = buffer.toString();
			OAIPage page = new OAIPage(xml);
//			OAIPage page = new OAIPage(myXML);

			System.out.println(page.getResuptionToken()+" - "+page.getCursor()+" : "+page.getRecords().size()+"/"+page.getCompleteListSize());
//			System.out.println(page.getXML());

			String resumptionToken = page.getResuptionToken();
			String prevResumptionToken="";

			long start = System.currentTimeMillis();
			long startm = start;
			long s = start;
			int count=0;
			int nb=page.getRecords().size();

			//Create a transformation code 
			String code =	"auteur  <= $_(dc:creator)[ && ]\n" +
							"auteurs  <dc:creator>\n" +
							"titre <dc:title>\n";/* +
							"dateSource <= $_(dc:date) - $_(dc:source)\n";*/
			System.out.println(code);
			RFHarvesterCodeTransformator Transformator = new RFHarvesterCodeTransformator(code);

			for(OAIRecord record : page.getRecords())
			{
//				System.out.println(Transformator.transform(record.getMetadata().getValues()));
				resultFileOutputStream.write((Transformator.transform(record.getMetadata().getValues())+"\n").getBytes());
			}
			resultFileOutputStream.flush();

			while(!resumptionToken.isEmpty() && resumptionToken!=null)
			{
//				System.out.println("Cur resumption: " + resumptionToken);
				url = new URL(oaiLink + "?verb=ListRecords&resumptionToken=" + resumptionToken);
				
//				IS = url.openStream();
				
				con = url.openConnection();
				IS = con.getInputStream();

				ptr = 0;
				buffer = new StringBuffer();
				while ((ptr = IS.read()) != -1)
				    buffer.append((char)ptr);
				//System.out.println(buffer);
				xml = buffer.toString();
//				System.out.println(xml);
//				OAIPage oldpage = page;
				try
				{
					page = new OAIPage(xml);
					for(OAIRecord record : page.getRecords())
					{
//						System.out.println(Transformator.transform(record.getMetadata().getValues()));
						resultFileOutputStream.write((Transformator.transform(record.getMetadata().getValues())+"\n").getBytes());
					}
					resultFileOutputStream.flush();
				}
				catch(Exception e)
				{
					System.out.println("FAIL!!!");
//					System.out.println(oldpage.getXML());
					long endTime = System.currentTimeMillis();
					System.out.println(count);
					System.out.println(s);
					System.out.println("Total duration: " + ((endTime-s)/1000)+" secs : " + prevResumptionToken + " - " + resumptionToken);
					System.out.println(nb + " records parsed");
					e.printStackTrace();
					System.exit(0);
				}

				prevResumptionToken=resumptionToken;
				resumptionToken = page.getResuptionToken();
				if(resumptionToken == null)
					break;

				System.out.println(page.getResuptionToken()+" - "+page.getCursor()+" : "+(Integer.parseInt(page.getCursor())+page.getRecords().size())+"/"+page.getCompleteListSize());
				resultFileOutputStream.write((page.getResuptionToken()+" - "+page.getCursor()+" : "+(Integer.parseInt(page.getCursor())+page.getRecords().size())+"/"+page.getCompleteListSize()+"\n").getBytes());
				resultFileOutputStream.flush();

				nb+=page.getRecords().size();
				count++;
				if(count%400==0)
				{
					long elapsedTimeMillis = System.currentTimeMillis()-startm;
					long elapseMin = (elapsedTimeMillis/(60*1000));
					long elapseSec = elapsedTimeMillis-(elapseMin*60*1000);
					elapseSec = elapseSec/1000;
					elapsedTimeMillis = System.currentTimeMillis()-start;
					System.out.println(count+": "+(elapsedTimeMillis/1000)+" secs - "+elapseMin+"min"+elapseSec+"sec : " + prevResumptionToken + " - " + resumptionToken);
					resultFileOutputStream.write((count+": "+(elapsedTimeMillis/1000)+" secs - "+elapseMin+"min"+elapseSec+"sec : " + prevResumptionToken + " - " + resumptionToken+"\n").getBytes());
					resultFileOutputStream.flush();
					startm = start = System.currentTimeMillis();
				}
				else if(count%100==0)
				{
					long elapsedTimeMillis = System.currentTimeMillis()-start;
					System.out.println(count+": "+(elapsedTimeMillis/1000)+" secs : " + prevResumptionToken + " - " + resumptionToken);
					resultFileOutputStream.write((count+": "+(elapsedTimeMillis/1000)+" secs : " + prevResumptionToken + " - " + resumptionToken+"\n").getBytes());
					resultFileOutputStream.flush();
					start = System.currentTimeMillis();
				}
			}
			long e = System.currentTimeMillis();
			System.out.println(count);
			System.out.println(s);
			System.out.println(e);
			System.out.println("Total duration: " + ((e-s)/1000)+" secs : " + prevResumptionToken + " - " + resumptionToken);
			System.out.println(nb + " records parsed");

			resultFileOutputStream.write((count+"\n").getBytes());
			resultFileOutputStream.write((s+"\n").getBytes());
			resultFileOutputStream.write((e+"\n").getBytes());
			resultFileOutputStream.write(("Total duration: " + ((e-s)/1000)+" secs : " + prevResumptionToken + " - " + resumptionToken+"\n").getBytes());
			resultFileOutputStream.write((nb + " records parsed\n").getBytes());
			resultFileOutputStream.flush();
			resultFileOutputStream.close();
		}
		catch(MalformedURLException e)
		{
			e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}
