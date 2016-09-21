-- MySQL dump 10.14  Distrib 5.5.44-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: bpiharvester
-- ------------------------------------------------------
-- Server version	5.5.44-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `configuration`
--

DROP TABLE IF EXISTS `configuration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `configuration` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `harvester_ID` int(10) unsigned NOT NULL,
  `collection_ID` int(10) unsigned NOT NULL DEFAULT '0',
  `URL` text,
  `URLADDITION` text,
  `transformation_ID` int(10) unsigned DEFAULT NULL,
  `dispo_sur_poste` text,
  `dispo_bibliotheque` text,
  `dispo_access_libre` text,
  `dispo_avec_reservation` text,
  `dispo_avec_access_autorise` text,
  `dispo_broadcast_group` text,
  `default_document_type_ID` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `harvester_ID` (`harvester_ID`),
  KEY `transformation_ID` (`transformation_ID`),
  KEY `default_document_type_ID` (`default_document_type_ID`),
  CONSTRAINT `configuration_ibfk_1` FOREIGN KEY (`harvester_ID`) REFERENCES `harvester` (`ID`),
  CONSTRAINT `configuration_ibfk_2` FOREIGN KEY (`transformation_ID`) REFERENCES `transformation` (`ID`),
  CONSTRAINT `configuration_ibfk_3` FOREIGN KEY (`default_document_type_ID`) REFERENCES `document_type` (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `configuration`
--

LOCK TABLES `configuration` WRITE;
/*!40000 ALTER TABLE `configuration` DISABLE KEYS */;
INSERT INTO `configuration` VALUES (1,'portfoliodw',1,5,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(4,'cairn.info Encyclo',3,142,'http://oai.cairn.info/oai.php','&set=Encyclo',2,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(5,'cairn.info Magazines',3,145,'http://oai.cairn.info/oai.php','&set=m:56',2,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',2),(6,'cairn.info Revues',3,146,'http://oai.cairn.info/oai.php','&set=b:2',2,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',2),(8,'Revues.org',3,141,'http://oai.openedition.org/','&set=journals',9,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',2),(9,'ged',3,6,'http://oai.openedition.org/','&set=books',1,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(10,'irevues.inist',3,80,'http://documents.irevues.inist.fr/dspace-oai/request','',10,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',2),(11,'Dalloz BN',4,147,'http://logistic.book-vision.com/services/oai/act68.php',NULL,1,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(12,'Gallica:periodiques',3,124,'http://oai.bnf.fr/oai2/OAIHandler','&set=gallica:typedoc:periodiques:titres',6,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',4),(13,'gallica:typedoc:partitions',3,56,'http://oai.bnf.fr/oai2/OAIHandler','&set=gallica:typedoc:partitions',6,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',10),(14,'NP_citemusique',3,118,'http://194.250.19.147/scripts/oaiserver.asp','&set=NP',5,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',21),(15,'EU_citemusique',3,120,'http://194.250.19.147/scripts/oaiserver.asp','&set=EU',5,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',12),(16,'bibliovox',3,134,'http://oai-bibliovox.cyberlibris.fr/oai.aspx','',1,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(17,'MU_citemusique',3,121,'http://194.250.19.147/scripts/oaiserver.asp','&set=MU',5,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',9),(18,'IN_citemusique',3,119,'http://194.250.19.147/scripts/oaiserver.asp','&set=IN',5,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',15),(19,'IM_cite_musique',3,116,'http://194.250.19.147/scripts/oaiserver.asp','&set=IM',5,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',9),(20,'SER Bpi',3,140,'http://api.archives-ouvertes.fr/oai/hal/','&set=collection:SER-BPI',7,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(22,'webtv_webradio',3,101,'http://archives-sonores.bpi.fr/oai.php','',8,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',14),(23,'cairn.info EcoSocPol',3,150,'http://oai.cairn.info/oai.php','&set=o:90',2,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(24,'OpenEdition Books',3,148,'http://oai.openedition.org/','&set=books',9,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',3),(25,'gallica:typedoc:cartes',3,151,'http://oai.bnf.fr/oai2/OAIHandler','&set=gallica:typedoc:cartes',6,'online','online','online','online','online','GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs',8);
/*!40000 ALTER TABLE `configuration` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `document_type`
--

DROP TABLE IF EXISTS `document_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `document_type` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` text,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=23 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `document_type`
--

LOCK TABLES `document_type` WRITE;
/*!40000 ALTER TABLE `document_type` DISABLE KEYS */;
INSERT INTO `document_type` VALUES (1,NULL),(2,'Article'),(3,'Livre numérique'),(4,'Revue, journal'),(5,'Livre'),(6,'Encyclopédie et dictionnaire'),(7,'Dossier de presse'),(8,'Carte'),(9,'Image'),(10,'Partition et méthode'),(11,'Didacticiel'),(12,'Musique'),(13,'Livre audio'),(14,'Débat et enregistrement'),(15,'Vidéo'),(16,'Site'),(17,'Base de données'),(18,'Evènement'),(20,'Référence d\'article'),(21,'Brochure'),(22,'BD');
/*!40000 ALTER TABLE `document_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `transformation`
--

DROP TABLE IF EXISTS `transformation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `transformation` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `code` text NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `transformation`
--

LOCK TABLES `transformation` WRITE;
/*!40000 ALTER TABLE `transformation` DISABLE KEYS */;
INSERT INTO `transformation` VALUES (1,'Transfo OAI','titres <dc:title>\nsujets <dc:subject xml:lang=\"fr\">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'),(2,'Transfo Cairn OAI','titres <dc:title>\nrelations <dc:relation>\nsujets <dc:subject xml:lang=\"fr\">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ndescriptions <dc:description>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nURL <dc:identifier>\n'),(5,'Transfo  Cite de la Musique OAI','titres <dc:title>\nrelations <dc:relation>\nsujets <dc:subject xml:lang=\"fr\">\nsujets <dc:subject>\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ndescriptions <dc:description>\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'),(6,'Transfo GALLICA OAI','titres <dc:title>\nrelations <dc:relation>\nsujets <dc:subject xml:lang=\"fre\">\nsujets <dc:subject>\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ndescriptions <dc:description>\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'),(7,'Transfo  SER Bpi Etudes et recherche OAI','titres <dc:title xml:lang=\"fr\">\nrelations <dc:relation>\nsujets <dc:subject xml:lang=\"fr\">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nURL <dc:identifier>'),(8,'Transfo Web TV OAI','titres <dc:title>\nsujets <dc:subject>\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\ndescriptions <dc:description>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nURL <dc:identifier>\nvignettes <dc:relation>\n'),(9,'Transfo Revues.org et OE Books OAI','titres <dc:title>\nsujets <dc:subject xml:lang=\"fr\">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang=\"fr\">\ndescriptions <dc:description>\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'),(10,'Transfo  I-Revues OAI','titres <dc:title>\nsujets < dc:subject>\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description >\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nURL <dc:identifier>\n'),(11,'Transfo Persee OAI','sources <= Dans $_(dcterms:bibliographicCitation.jtitle), n $_(dcterms:bibliographicCitation.volume), $_(dcterms:bibliographicCitation.issue), $_(dc:date), pp.$_(dcterms:bibliographicCitation.spage)-$_(dcterms:bibliographicCitation.epage)');
/*!40000 ALTER TABLE `transformation` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `harvester`
--

DROP TABLE IF EXISTS `harvester`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `harvester` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` text NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `harvester`
--

LOCK TABLES `harvester` WRITE;
/*!40000 ALTER TABLE `harvester` DISABLE KEYS */;
INSERT INTO `harvester` VALUES (1,'Portfolio'),(2,'Authorities'),(3,'OAI_DC'),(4,'ONIX_DC');
/*!40000 ALTER TABLE `harvester` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-09-08 11:34:25
