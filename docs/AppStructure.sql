DROP DATABASE IF EXISTS credit_portfolio;
CREATE DATABASE credit_portfolio CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE credit_portfolio;

CREATE TABLE companies (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Social_Reason VARCHAR(100) NOT NULL,
    CUIT BIGINT NOT NULL,
    Advance DECIMAL(15,2) NOT NULL DEFAULT 0);


CREATE TABLE business_plan (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    ID_Company INT NOT NULL,
    FOREIGN KEY (ID_Company) REFERENCES companies(ID) ON UPDATE CASCADE,
    Detail VARCHAR(200),
    Date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    Comission DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_6 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_9 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_12 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_15 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_18 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_24 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_36 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_48 DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_Collection DECIMAL(10,8) NOT NULL DEFAULT 0,
    Comission_Extra DECIMAL(10,8) NOT NULL DEFAULT 0);


CREATE TABLE provinces (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL);

CREATE TABLE customers (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    CUIL BIGINT NOT NULL,
    DNI BIGINT NOT NULL,
    Last_Name VARCHAR(100) NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Gender ENUM('M', 'F', 'NB', 'T', 'G', 'A', 'O') DEFAULT 'O',
    Date_Birth DATETIME,
    Marital_Status Enum('SINGLE', 'COHABITATION', 'MARRIED', 'WIDOW', 'DIVORCE'),
    Age_at_Discharge TINYINT,
    Country VARCHAR(100) NOT NULL DEFAULT 'Argentina',
    ID_Province INT,
    FOREIGN KEY (ID_Province) REFERENCES provinces(ID) ON DELETE SET NULL ON UPDATE CASCADE,
    Locality VARCHAR(100),
    Street VARCHAR(100),
    Nro INT,
    CP INT,
    Feature INT,
    Telephone VARCHAR(25),
    Seniority INT, Salary DECIMAL(20,2),
    CBU VARCHAR(22),
    Collection_Entity VARCHAR(100),
    Employer VARCHAR(100),
    Dependence VARCHAR(100),
    CUIT_Employer VARCHAR(22),
    ID_Empl_Prov INT,
    FOREIGN KEY (ID_Empl_Prov) REFERENCES provinces(ID) ON DELETE SET NULL ON UPDATE CASCADE, Empl_Loc VARCHAR(100),
    Empl_Adress VARCHAR(100),
    Last_Update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

CREATE TABLE portfolio_purchases(
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Date DATETIME NOT NULL,
    ID_Company INT NOT NULL DEFAULT 0,
    FOREIGN KEY (ID_Company) REFERENCES companies(ID) ON UPDATE CASCADE,
    TNA DECIMAL(10,8) NOT NULL,
    Buyback TINYINT NOT NULL DEFAULT 0,
    Resource TINYINT NOT NULL DEFAULT 1,
    IVA TINYINT NOT NULL DEFAULT 0);

CREATE TABLE portfolio_sales (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Date DATETIME NOT NULL,
    ID_Company INT NOT NULL DEFAULT 0,
    FOREIGN KEY (ID_Company) REFERENCES companies(ID) ON UPDATE CASCADE,
    TNA DECIMAL(10,8) NOT NULL,
    Resource TINYINT NOT NULL DEFAULT 1,
    IVA TINYINT NOT NULL DEFAULT 0);

CREATE TABLE credits (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    ID_External VARCHAR(10),
    ID_Client INT NOT NULL,
    FOREIGN KEY (ID_Client) REFERENCES customers(ID) ON UPDATE CASCADE,
    Date_Settlement DATETIME NOT NULL,
    ID_BP INT,
    FOREIGN KEY (ID_BP) REFERENCES business_plan(ID) ON UPDATE CASCADE,
    Cap_Requested DECIMAL(15,2) NOT NULL DEFAULT 0,
    Cap_Grant DECIMAL(15,2) NOT NULL,
    N_Inst INT NOT NULL,
    First_Inst_Purch INT,
    TEM_W_IVA DECIMAL(10,8) NOT NULL,
    V_Inst DECIMAL(15,2) NOT NULL DEFAULT 0,
    First_Inst_Sold INT ,
    D_F_Due DATETIME NOT NULL,
    ID_Purch INT,
    FOREIGN KEY (ID_Purch) REFERENCES portfolio_purchases(ID) ON UPDATE CASCADE,
    ID_Sale INT,
    FOREIGN KEY (ID_Sale) REFERENCES portfolio_sales(ID) ON UPDATE CASCADE);

CREATE TABLE installments (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    ID_Op INT NOT NULL,
    FOREIGN KEY (ID_Op) REFERENCES credits(ID) ON UPDATE CASCADE,
    Nro_Inst INT NOT NULL,
    D_Due DATETIME NOT NULL,
    Capital DECIMAL(15,2) NOT NULL,
    Interest DECIMAL(15,2) NOT NULL,
    IVA DECIMAL(15,2) NOT NULL,
    Total DECIMAL(15,2) NOT NULL,
    ID_Owner INT,
    FOREIGN KEY (ID_Owner) REFERENCES companies(ID) ON UPDATE CASCADE);

CREATE TABLE collection (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    ID_Inst INT,
    FOREIGN KEY (ID_Inst) REFERENCES installments(ID) ON UPDATE CASCADE,
    D_Emission DATETIME NOT NULL,
    Type_Collection ENUM('COMUN', 'ANTICIPADA', 'PARCIAL', 'REDONDEO', 'PENALTY', 'CAN. ANT.', 'BON. CAN. ANT.', 'REVERSA', 'NO COMPRADA', 'RECURSO'),
    Capital DECIMAL(15,2) NOT NULL,
    Interest DECIMAL(15,2) NOT NULL,
    IVA DECIMAL(15,2) NOT NULL,
    Total DECIMAL(15,2) NOT NULL);

CREATE TABLE settings (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Detail VARCHAR(100) NOT NULL,
    Type ENUM('T', 'I', 'F', 'D') NOT NULL DEFAULT 'T',
    Value TEXT NOT NULL);

CREATE TABLE portfolio_sales_generated (
    ID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    TNA DECIMAL(10,8) NOT NULL);