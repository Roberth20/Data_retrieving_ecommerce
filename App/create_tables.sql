CREATE TABLE IF NOT EXISTS Atributos_Falabella(
    id int NOT NULL AUTO_INCREMENT,
    Label varchar(256) NOT NULL,
    Category VARCHAR(256) NOT NULL,
    GroupName varchar(256) NULL,
    isMandatory BOOL NOT NULL,
    isGlobalAttribute BOOL NOT NULL,
    Description MEDIUMTEXT NULL,
    ProductType VARCHAR(64) NULL,
    AttributeType VARCHAR(12) NOT NULL,
    Options TEXT(35000) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Atributos_MercadoLibre(
    id INT NOT NULL AUTO_INCREMENT,
    Label VARCHAR(120) NOT NULL,
    Category VARCHAR(60) NOT NULL,
    Tags VARCHAR(120) NOT NULL,
    Hierarchy VARCHAR(20) NOT NULL,
    AttributeType VARCHAR(15) NOT NULL,
    GroupName VARCHAR(64) NOT NULL,
    Options TEXT(35000) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Atributos_Paris(
    id INT NOT NULL AUTO_INCREMENT,
    Label VARCHAR(64) NOT NULL,
    Category VARCHAR(40) NOT NULL,
    Family VARCHAR(20) NOT NULL,
    AttributeType VARCHAR(16) NOT NULL,
    GroupName VARCHAR(20) NULL,
    isMandatory BOOL NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Atributos_Ripley(
    id INT NOT NULL AUTO_INCREMENT,
    Label VARCHAR(64) NOT NULL,
    Category VARCHAR(80) NOT NULL,
    RequirementLevel CHAR(8) NOT NULL,
    AttributeType VARCHAR(10) NOT NULL,
    Options VARCHAR(120) NULL,
    Variant BOOL NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS Mapeo_Falabella(
    id INT NOT NULL AUTO_INCREMENT,
    Mapeo VARCHAR(45) NOT NULL,
    Atributo VARCHAR(36) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS Mapeo_MercadoLibre(
    id INT NOT NULL AUTO_INCREMENT,
    Mapeo VARCHAR(70) NOT NULL,
    Atributo VARCHAR(40) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS Mapeo_Paris(
    id INT NOT NULL AUTO_INCREMENT,
    Mapeo VARCHAR(50) NOT NULL,
    Atributo VARCHAR(50) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS Mapeo_Ripley(
    id INT NOT NULL AUTO_INCREMENT,
    Mapeo VARCHAR(45) NOT NULL,
    Atributo VARCHAR(40) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS Mapeo_Categorias(
    id INT NOT NULL AUTO_INCREMENT,
    Multivende VARCHAR(150) NOT NULL,
    MercadoLibre VARCHAR(150) NOT NULL,
    Falabella VARCHAR(220) NOT NULL,
    Ripley VARCHAR(200) NOT NULL,
    Paris VARCHAR(120) NOT NULL,
    Paris_Familia VARCHAR(30) NOT NULL,
    PRIMARY KEY(id)
)