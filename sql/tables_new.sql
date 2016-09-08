/*set @orig_mode = @@global.sql_mode;

set @@global.sql_mode = "MYSQL40";*/

DROP TABLE IF EXISTS cron_line;
DROP TABLE IF EXISTS configuration;
DROP TABLE IF EXISTS harvester;
DROP TABLE IF EXISTS transformation;
DROP TABLE IF EXISTS document_type;
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS status;

CREATE TABLE status
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	PID INTEGER UNSIGNED,
	configuration VARCHAR(255) NOT NULL DEFAULT 'Configuration inconnue',
	status VARCHAR(255) NOT NULL DEFAULT 'Etat inconnu',
	harvested INTEGER UNSIGNED NOT NULL DEFAULT 0,
	startDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	endDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	message TEXT,
	PRIMARY KEY (ID)
);

CREATE TABLE logs
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	date TIMESTAMP,
	thread INTEGER UNSIGNED,
	type VARCHAR(9),
	message TEXT,
	PRIMARY KEY (ID)
);

CREATE TABLE transformation
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	name TEXT,
	code TEXT NOT NULL,
	PRIMARY KEY (ID)
);

INSERT INTO
	transformation(name, code)
VALUES
(
	'TransfoOAI',
	'titres <dc:title>\nrelations <dc:relation>\nsujets <dc:subject xml:lang="fr">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang="fr">\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'
);

INSERT INTO
	transformation(name, code)
VALUES
(
	'TransfoCairnOAI',
	'titres <dc:title>\nrelations <dc:relation>\nsujets <dc:subject xml:lang="fr">\nauteurs <dc:creator>\ncontributeurs <dc:contributor>\nediteurs <dc:publisher>\ntypes <dc:type>\nrelations <dc:relation>\ndescriptions <dc:description xml:lang="fr">\ndescriptions <dc:description>\ncoverages <dc:coverage>\nlangues <dc:language>\ndates <dc:date>\nformats <dc:format>\nsources <dc:source>\nrights <dc:rights>\nvignettes <dc:thumbnail>\nURL <dc:identifier>\n'
);

INSERT INTO
	transformation(name, code)
VALUES
(
	'TransfoGallica',
	'auteur <= $_(dc:creator)[ && ]\nauteurs <dc:creator>\ntitre <dc:title>\n'
);

CREATE TABLE harvester
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	name TEXT NOT NULL,
	PRIMARY KEY (ID)
);

INSERT INTO harvester(name) VALUES ('Portfolio');
INSERT INTO harvester(name) VALUES ('Authorities');
INSERT INTO harvester(name) VALUES ('OAI_DC');
INSERT INTO harvester(name) VALUES ('ONIX_DC');

CREATE TABLE document_type
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	type TEXT,
	PRIMARY KEY (ID)
);

INSERT INTO document_type(type) VALUES (NULL);
INSERT INTO document_type(type) VALUES ('Article');
INSERT INTO document_type(type) VALUES ('Livre numérique');
INSERT INTO document_type(type) VALUES ('Revue, journal');

INSERT INTO document_type(type) VALUES ('Livre');
INSERT INTO document_type(type) VALUES ('Encyclopédie et dictionnaire');
INSERT INTO document_type(type) VALUES ('Dossier de presse');
INSERT INTO document_type(type) VALUES ('Carte');
INSERT INTO document_type(type) VALUES ('Image');
INSERT INTO document_type(type) VALUES ('Partition et méthode');
INSERT INTO document_type(type) VALUES ('Didacticiel');
INSERT INTO document_type(type) VALUES ('Musique');
INSERT INTO document_type(type) VALUES ('Livre audio');
INSERT INTO document_type(type) VALUES ('Débat et enregistrement');
INSERT INTO document_type(type) VALUES ('Vidéo');
INSERT INTO document_type(type) VALUES ('Site');
INSERT INTO document_type(type) VALUES ('Base de données');
INSERT INTO document_type(type) VALUES ('Evènement');
INSERT INTO document_type(type) VALUES ('Référence d''article');
INSERT INTO document_type(type) VALUES ('Brochure');
INSERT INTO document_type(type) VALUES ('BD');

CREATE TABLE configuration
(
	ID INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, /**/
	name TEXT,
	harvester_ID INTEGER UNSIGNED NOT NULL,
	collection_ID INTEGER UNSIGNED NOT NULL DEFAULT 0,
	URL TEXT,
	URLADDITION TEXT,
	transformation_ID INTEGER UNSIGNED,/* NOT NULL,*/

	dispo_sur_poste TEXT,
	dispo_bibliotheque TEXT,
	dispo_access_libre TEXT,
	dispo_avec_reservation TEXT,
	dispo_avec_access_autorise TEXT,
	dispo_broadcast_group TEXT,
	default_document_type_ID INTEGER UNSIGNED,/* NOT NULL,*/

	/*debug_FLAG BOOL NOT NULL DEFAULT FALSE,
	no_log_FLAG BOOL NOT NULL DEFAULT FALSE,*/
	PRIMARY KEY (ID),
	FOREIGN KEY (harvester_ID) REFERENCES harvester(ID),
	FOREIGN KEY (transformation_ID) REFERENCES transformation(ID),
	FOREIGN KEY (default_document_type_ID) REFERENCES document_type(ID)
);

CREATE TABLE cron_line
(
	ID  INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
	m   INTEGER UNSIGNED DEFAULT NULL,
	h   INTEGER UNSIGNED DEFAULT NULL,
	dom INTEGER UNSIGNED DEFAULT NULL,
	mon INTEGER UNSIGNED DEFAULT NULL,
	dow INTEGER UNSIGNED DEFAULT NULL,
	configuration_ID INTEGER UNSIGNED NOT NULL,
	PRIMARY KEY (ID),
	FOREIGN KEY (configuration_ID) REFERENCES configuration(ID)
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID
)
VALUES
(
	'portfoliodw',
	1,
	5
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'cairn.info',
	3,
	26,
	'http://oai.cairn.info/oai.php',
	1,
	1,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'OAI BNF',
	3,
	133,
	'http://oai.bnf.fr/oai2/OAIHandler',
	1,
	1,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'cairn.info Encyclo',
	3,
	142,
	'http://oai.cairn.info/oai.php',
	'&set=Encyclo',
	2,
	3,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'cairn.info Magazines',
	3,
	145,
	'http://oai.cairn.info/oai.php',
	'&set=m:56',
	2,
	2,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'cairn.info Revues',
	3,
	146,
	'http://oai.cairn.info/oai.php',
	'&set=b:2',
	2,
	2,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'openedition',
	3,
	125,
	'http://oai.openedition.org/',
	1,
	1,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'Revues.org',
	3,
	141,
	'http://oai.openedition.org/',
	'&set=journals',
	1,
	2,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'OpenEdition Books',
	3,
	148,
	'http://oai.openedition.org/',
	'&set=books',
	2,
	3,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'irevues.inist',
	3,
	80,
	'http://documents.irevues.inist.fr/dspace-oai/request',
	1,
	4,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'Dalloz BN',
	4,
	147,
	'http://logistic.book-vision.com/services/oai/act68.php',
	1,
	3,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

INSERT INTO
configuration
(
	name,
	harvester_ID,
	collection_ID,
	URL,
	URLADDITION,
	transformation_ID,
	default_document_type_ID,
	dispo_sur_poste, dispo_bibliotheque, dispo_access_libre, dispo_avec_reservation, dispo_avec_access_autorise, dispo_broadcast_group
)
VALUES
(
	'Gallica:periodiques',
	3,
	124,
	'http://oai.bnf.fr/oai2/OAIHandler',
	'&set=gallica:typedoc:periodiques:titres',
	2,
	4,
	'online', 'online', 'online', 'online', 'online', 'GD-Articles de presse,GD-Ged,GD-Journaux en ligne,GD-Mezzanine,GD-Réel,GD-Tous secteurs'
);

/*set @@global.sql_mode = @orig_mode;*/
