# FinancialApp

**FinancialApp** is an application designed to manage a credit portfolio. The platform enables you to oversee both credits granted by the company and those acquired from third parties. Future versions will include functionalities for managing additional investment types and handling debt to finance these investments.

## Project Structure

The project structure is organized as follows:

```
FinancialApp/
├── app/
│   ├── __init__.py
│   └── modules/
│       ├── __init__.py
│       └── database
│           ├── __init__.py
│           ├── companies.py
│           ├── connection.py
│           ├── credit_manager.py
│           ├── customers.py
│           ├── folder_manager.py
│           └── structur_databases.py
├── docs/
│   ├── requirements.txt
│   └── AppStructure.sql
├── logs/
│   ├── app.log
│   ├── log.py
│   └── run_log.py
├── notebooks/
│   ├── Analysis.ipynb
│   └── Start_DataBases.ipynb
├── fa_env/
├── .gitignore
└── README.md
```

### Folder Description

- **app/:** Contains the main source code of the application, organized into modules necessary for its functionality.
- **docs/:** Includes documents related to client data and the structure of the credit portfolio.
- **inputs/:** Contains input files, such as CSV and Excel documents, required for initializing or updating the credit portfolio and related data. These files serve as data sources for the application.
- **logs/:** Houses log files and scripts for managing logs and the database.
- **notebooks/:** Contains Jupyter notebooks for data analysis and interactive code execution.
- **fa_env/:** Virtual environment for managing the project's dependencies.


## Installation

### Clone the repository:

```bash
git clone https://github.com/JuanMCarini/FinancialApp.git
```

### Navigate to the project directory:
```bash
cd FinancialApp
```

### Set Up the Virtual Environment and Install Dependencies
```bash
python -m venv fa_env
source fa_env/Script/activate
python.exe -m pip install --upgrade pip
pip install -r docs/requirements.txt
```

## Usage

### Running the Application

The application enables the management and visualization of credit information through its modules. The main components of its workflow include:

* **log.py:** To maintain a record of actions and modifications in the project and ensure everything works after changes.
* **run_log.py:** Starts the log management.
* **Start_DataBases.ipynb:** Executes data analysis and queries on the credit database and tests new functions.
* **Analysis.ipynb:** For future analysis of the database, such as "late payments."

## Modules and Features

Next, we will describe the different modules and the functions within them.

### Module Description: ```structur_databases.py```

The ```structur_databases.py```* module defines the SQLAlchemy ORM classes that represent the structure of the database for the FinancialApp project. It includes models for the Province, Customer, and Company tables, as well as the relationships between them. These models define how data is stored and retrieved from the database.

#### **Key Elements:**

1. **Imports:**
    * ```sqlalchemy.ext.declarative.declarative_base```: Used to create a base class for the ORM models.
    * ```sqlalchemy.Column```, ```Integer```, ```BigInteger```, ```String```, ```DateTime```, ```DECIMAL```, ```ForeignKey```, ```Numeric```: These are the column types and constraints used to define the structure of each table in the database.
    * ```sqlalchemy.orm.relationship```: Defines the relationships between tables, such as foreign keys.

2. **Classes:**

    * ```Province```: Represents a province in Argentina and is used in the Customer table to link each customer to their province.
    * ```Customer```: Represents a customer in the system. This table contains various fields such as:
        * ```CUIL```: A unique identifier for the customer.
        * ```DNI```: The customer's national identity number.
        * ```Last_Name```, ```Name```: The customer's last and first names.
        * ```Gender```: The customer's gender.
        * ```Date_Birth```, ```Age_at_Discharge```: The customer's date of birth and age at account discharge.
        * ```ID_Province```: A foreign key linking the customer to a province from the Province table.
        * ```Telephone```, ```Salary```, ```CBU```: Contact information, salary, and bank account details.
        * ```Employer```, ```Dependence```, ```CUIT_Employer```: Employment-related information.
        * ```Last_Update```: The timestamp for when the customer's record was last updated.

        The Customer table has two relationships with the Province table:
        * One for the customer's province (```ID_Province```).
        * Another for the employer's province (```ID_Empl_Prov```).
    * Company: Represents a company in the system. This table includes:
        * ```CUIT```: The company's tax identification number.
        * ```Advance```: The company's advance amount (e.g., payments or debt management).

### Code Walkthrough:

1. **Province Class:**
    * This class represents the provinces in Argentina, with the following attributes:
        * ```ID```: A unique identifier for the province.
        * ```Name```: The name of the province.
        
    The class defines a string representation method (```__repr__```) to help with debugging or logging, which provides a readable format of the province's details.
    
    ```python
    class Province(Base):
        __tablename__ = 'provinces'
        ID = Column(Integer, primary_key=True, autoincrement=True)
        Name = Column(String(100), nullable=False)

        def __repr__(self):
            return f"<Province(ID={self.ID}, Name='{self.Name}')>"
    ```

2. **Customer Class:**

    * This is the core class for representing customer data, with the following key features:
        * ```CUIL``` and ```DNI```: Unique identifiers for the customer.
        * ```Last_Name```, ```Name```: The customer's personal information.
        * ```Gender```: The gender of the customer, represented as a two-character string.
        * ```Date_Birth```, ```Age_at_Discharge```: Dates related to the customer's birth and discharge.
        * ```ID_Province```, ```ID_Empl_Prov```: Foreign keys to the Province table that link the customer to their home province and employer's province.
        * ```Last_Update```: The timestamp indicating the last time the customer's record was updated.

    * The relationships with the Province table are defined via SQLAlchemy's relationship:
        * ```province```: Links the Customer to a Province using the ```ID_Province```.
        * ```employer_province```: Links the Customer to the Province where their employer is based using the ```ID_Empl_Prov```.

    ```python
    class Customer(Base):
        __tablename__ = 'customers'

        ID = Column(Integer, primary_key=True, autoincrement=True)
        CUIL = Column(BigInteger, nullable=False)
        DNI = Column(BigInteger, nullable=False)
        Last_Name = Column(String(100), nullable=False)
        Name = Column(String(100), nullable=False)
        Gender = Column(String(2), nullable=False, default='O')
        Date_Birth = Column(DateTime)
        Age_at_Discharge = Column(Integer)
        ID_Province = Column(Integer, ForeignKey('provinces.ID', ondelete='SET NULL', onupdate='CASCADE'))
        Locality = Column(String(100))
        Street = Column(String(100))
        Nro = Column(Integer)
        CP = Column(Integer)
        Feature = Column(Integer)
        Telephone = Column(BigInteger)
        Seniority = Column(Integer)
        Salary = Column(DECIMAL(20, 2))
        CBU = Column(String(22))
        Collection_Entity = Column(String(100))
        Employer = Column(String(100))
        Dependence = Column(String(100))
        CUIT_Employer = Column(String(22))
        ID_Empl_Prov = Column(Integer, ForeignKey('provinces.ID', ondelete='SET NULL', onupdate='CASCADE'))
        Empl_Loc = Column(String(100))
        Empl_Adress = Column(String(100))
        Last_Update = Column(DateTime)

        # Relationships
        province = relationship("Province", foreign_keys=[ID_Province])
        employer_province = relationship("Province", foreign_keys=[ID_Empl_Prov])
        ```

3. **Company Class:**
    
    The Company class represents a company in the system. It includes:
    * CUIT: The tax identification number of the company.
    * Advance: The amount of advance (used for debt or credit-related calculations).
    
    It also has a string representation method (```__repr__```), which is useful for debugging.

    ```python
    class Company(Base):
        __tablename__ = 'companies'
        ID = Column(Integer, primary_key=True, autoincrement=True)
        Social_Reason = Column(String(100), nullable=False)
        CUIT = Column(BigInteger, nullable=False)
        Advance = Column(Numeric(15, 2), nullable=False, default=0)

        def __repr__(self):
            return f"<Company(ID={self.ID}, Social_Reason='{self.Social_Reason}', CUIT={self.CUIT}, Advance={self.Advance})>"
    ```

#### **Key Features:**

* ORM Model for Database Interaction:
    * The ```Base``` class (from ```declarative_base()```) is used to define ORM models for interacting with the database.
* Foreign Key Relationships:
    * The ```Customer``` class has two foreign key relationships with the ```Province``` class:
        * One links to the customer's home province (```ID_Province```).
        * Another links to the employer's province (```ID_Empl_Prov```).
* Data Integrity with Constraints:
    * Foreign keys (```ForeignKey```) enforce data integrity between related tables (e.g., ensuring that a customer’s province matches an existing province record).
* String Representation:
    * The ```__repr__``` methods in the classes provide a way to represent instances of each class in a human-readable form, useful for debugging.

This module is crucial for defining the structure and relationships of the database used in the FinancialApp, and it provides a solid foundation for managing data related to customers, companies, and provinces.


### Module Description: connection.py

The connection.py module is responsible for establishing the connection to the MySQL database in the FinancialApp project. It loads the database configuration from a settings file and uses SQLAlchemy to create an engine for database interaction.

#### **Key Elements:**

1. Loading Configuration:
    * The module loads database configuration settings from a settings.json file located in the docs directory.
    * The settings include the database username (```user```), password (```password```), host (```host```), and charset (```charset```).
2. SQLAlchemy Setup:
    * The module imports necessary components from SQLAlchemy to establish a connection, such as create_engine and declarative_base.
    * The ```declarative_base``` is typically used for defining ORM classes (although it isn't directly used in this module, it could be useful for future ORM class creation).
3. Database Connection:
    * Using the loaded configuration, the module creates a connection string formatted for MySQL, utilizing mysql+pymysql as the database driver.
    * The create_engine function is used to initialize the database engine, which will be used for querying and interacting with the MySQL database.

#### **Behavior:**

* The module dynamically loads the database connection details from a JSON file (```settings.json```).
* It creates a connection string based on the provided configuration and establishes an engine to interact with the MySQL database using SQLAlchemy.

#### **Code Walkthrough:**

1. Loading Settings from settings.json:

    ```python
    with open("docs/settings.json", "r") as file:
        settings = json.load(file)
    ```

    This loads the configuration for the database connection, which includes the user, password, host, and charset, from the ```settings.json``` file.

2. Constructing the Connection String:

    ```python
    connection_string = f'mysql+pymysql://{user}:{password}@{host}/{database}?charset={charset}'
    ```

    This line builds the connection string needed to connect to the MySQL database. It combines the username, password, host, and charset into the correct format for MySQL connections via SQLAlchemy using the pymysql driver.

3. Creating the SQLAlchemy Engine:

    ```python
    engine = create_engine(connection_string)
    ```

    Finally, the create_engine function from SQLAlchemy is used to create an engine that can be used for querying the database, executing SQL commands, and interacting with the MySQL database.

#### **Key Features:**

* **Configurable Database Connection:** The connection to the MySQL database is dynamically configured via settings stored in a JSON file, which makes it easy to modify database credentials and connection settings.
* **SQLAlchemy Integration:** The use of SQLAlchemy’s create_engine allows the app to efficiently communicate with the MySQL database, using either raw SQL or SQLAlchemy’s ORM system (though ORM use isn’t explicitly shown in this module).
* **Security and Flexibility:** Storing sensitive database connection details in a JSON file allows for a flexible approach to configuration management, while also making it easier to modify settings without altering the source code.

This module is central to establishing a connection to the MySQL database and should be used across other modules to interact with the database efficiently.


### Module Description: companies.py

The ```companies.py``` module in the FinancialApp handles the management and registration of companies, as well as the creation and management of business plans associated with these companies. It includes two main functions: add_bussines_plan and add_company.
Functions:

1. **```add_bussines_plan```**
    
    This function is responsible for adding a business plan for a company to the database. It takes a variety of commission rates for different timeframes and associates them with a specific company using the company's ID. Before creating the business plan, the function validates that the company exists in the database. The business plan can optionally be saved to the database.

    * **Parameters:**
        * ```id_company```: The ID of the company to which the business plan will be associated.
        * ```detail```: A description or additional detail for the business plan.
        * ```com_6``` to ```com_48```: These are commission rates for various timeframes (from base commissions to 48 months).
        * ```save```: If ```True```, the business plan is saved to the database.

    * **Behavior:**
        * Verifies that the company exists by checking the provided company ID in the companies table.
        * Creates a new business plan with the given details.
        * Optionally saves the business plan to the database and prints a confirmation message, including the new plan’s ID and the associated company’s social reason (name).

    * **Exceptions:**
        * Raises a ```KeyError``` if the provided ```id_company``` does not exist in the companies table.

2. **```add_company```**

    This function is used to add a new company to the companies table. It validates the company’s CUIT (a unique identifier) and ensures that no duplicates are added. It can also create a business plan for the company if the bp parameter is set to True. After adding the company, the function prompts for additional details to generate the business plan.

    * **Parameters:**
        * ```social_reason```: The name or social reason of the company.
        * ```cuit```: The ```CUIT``` number of the company.
        * ```bp```: If ```True```, creates a business plan for the company.
        * ```save```: If ```True```, the company is saved to the database.

    * **Behavior:**
        * Validates the ```CUIT``` to ensure it has 11 digits.
        * Checks the companies table for any existing companies with the same ```CUIT```.
        * Adds the new company to the database and optionally associates a business plan.
        * Prompts the user for business plan details if bp is True and passes the input to the add_bussines_plan function for business plan creation.

    * Exceptions:
        * Raises a ```KeyError``` if the ```CUIT``` is invalid or if the company is already registered in the database.

#### **Key Features:**

* **Database Interaction:** The module interacts with the database using pandas and SQLAlchemy to manage company and business plan records.
* **Validation:** Both functions validate input data (such as company existence and CUIT validity) to ensure accurate records are added to the database.
* **Customizable Commission Rates:** The ```add_bussines_plan``` function allows flexible commission rate configuration for different timeframes, giving users control over how commissions are structured.
* **Optional Business Plan Creation:** The ```add_company``` function offers an option to create a business plan during company registration, streamlining the process of associating companies with business plans.

This module plays a critical role in managing the companies and business plans in the FinancialApp, ensuring that the app remains flexible and organized in handling business relationships.


### Module Description: ```customers.py```

The ```customers.py``` module is responsible for handling customer data within the FinancialApp project. It includes functions for adding customers to the system, updating existing customer records, and working with gender and province data. The module leverages Pandas and SQLAlchemy to interact with the database.

#### **Key Elements:**

1. **Imports:**
    * ```os```, ```pandas as pd```: Used for file handling and data manipulation.
    * ```engine```, ```sessionmaker```, ```update```: ```Imports from SQLAlchemy``` to interact with the database.
    * ```Customer```: The Customer ```class from structur_databases```, which define the structure of a customer record in the database.
2. Functions:
    * **```categorical_gender(gender: str) -> str```:**
        * Converts a full gender label to its corresponding abbreviation (e.g., "male" -> "M", "female" -> "F").
        * Raises an error if the gender is not valid.
    * **```add_customer(...)```:**
        * Adds a new customer to the database or updates an existing customer if their CUIL already exists.
        * Takes in a wide range of customer attributes and stores them in a Pandas DataFrame before inserting or updating the record in the database.
        * Uses ```categorical_gender()``` to validate and convert the gender field.
        * If the customer does not already exist, it inserts the new customer. If the customer exists, it compares old and new data and updates only the fields that have changed.
    * **```id_province(prov: str) -> int```:**
        * Takes a province name or alias (for Argentine provinces) and returns the corresponding ID from the provinces table in the database.
        * Uses a dictionary of aliases to match a variety of names for each province.

#### **Code Walkthrough:**

1. **```categorical_gender(gender: str) -> str```:**

    This function normalizes the gender string to a standard abbreviation using a dictionary mapping.
    ```python
    gender_map = {
    'male': 'M', 'female': 'F', 'other': 'O', 'non-binary': 'NB',
    'transgender': 'T', 'genderfluid': 'F', 'agender': 'A'
    }
    ```

    It validates the gender input and converts it to a predefined abbreviation, raising a ValueError if the gender is not recognized.

2. **```add_customer(...)```:**

    This function is responsible for adding new customers or updating existing ones in the database.
    * It first checks if the customer DataFrame is empty. If it is, it creates a new DataFrame with the provided customer data.
    * It then checks the database to see if a customer with the provided CUIL already exists.
        * If the customer is new, it inserts the data into the customers table.
        * If the customer already exists, it compares the old and new data and updates only the fields that differ (excluding the Last_Update field, which seems to be metadata).
    * SQLAlchemy’s sessionmaker is used to interact with the database and commit changes.

3. **```id_province(prov: str) -> int```:**

    The function matches the provided province name or alias to an official province name using an alias dictionary.
    * It then looks up the province ID in the provinces table (retrieved using ```pd.read_sql()```) and returns the corresponding ID. If the province is not found, it raises a KeyError.

#### **Key Features:**

* **Gender Conversion:** The ```categorical_gender``` function allows for flexible handling of gender data, accepting various forms and abbreviating them consistently.
* **Customer Data Management:** The ```add_customer``` function handles both new customer additions and the updating of existing records, ensuring that duplicate customers are not added to the database.
    * It uses a DataFrame for easy manipulation and comparison of customer data, leveraging pandas for data storage and transformation.
* **Province ID Lookup:** The ```id_province``` function ensures that province names or aliases are standardized, and the correct database ID is retrieved.
* **Database Interactions:** The module uses SQLAlchemy's ORM system to interact with the database, enabling both direct database queries (via ```pd.read_sql()```) and updates via SQLAlchemy's session-based system.

This module is crucial for managing customer data and ensures that data integrity is maintained in the FinancialApp database. It allows seamless insertion and updates while ensuring consistent data formatting.

### Module Overview: ```credit_manager.py```

The ```credit_manager.py``` module is responsible for managing credit operations, including the creation of credit records, generating installment schedules, and calculating the balance of credits based on collections. It works with financial data stored in a relational database using SQLAlchemy and Pandas, and performs operations involving financial calculations such as payment schedules and loan balances.

Here’s a breakdown of its functions:

#### **Key Functions**

1. **```create_installments():```**

    * **Purpose:** This function generates installment records for a given credit.
    * **Parameters:**
        * ```id_credit:``` The ID of the credit.
        * ```new_cr:``` A Pandas Series containing the credit details.
        * ```save:``` A flag to indicate whether the installments should be saved to the database.
    * **Returns:** A DataFrame containing the installment details for the specified credit.
    * **Details:**
        * The function uses the ```npf.pmt``` and ```npf.ipmt``` functions from the ```numpy_financial``` library to calculate the installment values and interest for each installment in the credit term.
        * It checks if the installments should be saved to the database and updates the ```installments``` table if the ```save``` flag is set to ```True```.

2. **```new_credit():```**

    * *Purpose:* This function creates a new credit record and its corresponding installment schedule.
    * *Parameters:*
        * Several parameters including ```id_customer```, ```Date_Settlement```, ```Cap_Requested```, ```Cap_Grant```, ```N_Inst```, ```TEM_W_IVA```, etc., which represent various details about the credit and its installments.
    * **Returns:**
        * A DataFrame with the new credit record.
        * A DataFrame with the generated installment schedule.
    * **Details:**
        * The function first validates the credit details and ensures that the installment value (```V_Inst```) matches the calculated value using the ```npf.pmt``` formula.
        * It retrieves some global settings (such as due day and grace periods) from the database.
        * It calculates the next due date based on the settlement date and determines the first installment due date, considering grace periods.
        * After preparing the credit data, the new credit is inserted into the ```credits``` table and the installments are generated and saved.

3. **```credits_balance():```**

    * *Purpose:* This function calculates the balance of credits by adjusting the installment amounts based on the amounts that have already been collected.
    * *Returns:* A DataFrame representing the updated balances for all installments, including columns for 'Capital', 'Interest', 'IVA', and 'Total'.
    * *Details:*
        * The function reads the ```collection``` and ```installments``` tables from the database and adjusts the installment amounts by subtracting the amounts collected.
        * The collection data is grouped by installment ID, and the adjustments are applied to the corresponding installments.

#### **Dependencies**

    * *Pandas:* Used for data manipulation and querying the database.
    * *numpy_financial (npf):* Provides financial functions such as ```pmt``` for calculating payment values and ```ipmt``` for calculating interest payments.
    * *dateutil.relativedelta:* Used to handle date arithmetic, specifically for calculating grace periods and due dates.

    The module is designed to handle operations related to credit management, such as setting up new credits with installment plans, calculating the balances of existing credits based on payments made, and managing related data within a SQL database.


#### **Code Review and Observations:**

1. **Imports:**
    * The imports are well organized, and the usage of ```pandas``` for data manipulation and ```numpy_financial``` for financial calculations is appropriate.
    * The ```dateutil.relativedelta``` module is used for date manipulations, specifically to calculate grace periods for the due dates.
2. **SQLAlchemy Integration:**
    * The use of ```pd.read_sql``` to interact with the database is effective for loading and manipulating data from SQL tables.
    * The ```engine``` is correctly imported from the connection module, ensuring smooth database interaction.
3. **Error Handling:**
    * In the ```new_credit``` function, there is validation to ensure that the installment value provided matches the calculated value, and checks are in place for the first due date being before the settlement date.
4. **Date Handling:**
    * The code handles dates well, using ```pd.Timestamp``` and ```pd.Period``` to manage the due dates and settlement dates. The use of ```relativedelta``` for applying grace periods is appropriate.
5. ***Data Consistency:***
    * When saving new records to the database (e.g., ```new_cr.to_sql()```), the ```if_exists='append'``` parameter ensures that data is added without overwriting existing records.

### Module Description: ```collection.py```

The ```collection.py``` module is a comprehensive utility for managing financial collections in the FinancialApp project. It provides functionalities for validating customer and credit identifiers, processing regular and early payments, handling penalties, reversing collections, and managing massive data collection tasks from external files. The module leverages SQLAlchemy and Pandas for database operations and data manipulation.

#### **Key Elements:**

1. **Enumerations and Error Handling:**

    * **```TypeDataCollection``` (Enum):**
        * Represents different types of identifiers used for collections (e.g., ```CUIL```, ```DNI```, ```ID_Op```, ```ID_Ext```).
        * Includes a static method ```validate``` to ensure identifier values are valid for the given type.
    * **Custom Exceptions:**
        * ```IdentifierError```: Raised when no credits are found for a given identifier.
        * ```ResourceError```: Raised when credits without recourse are unavailable for a customer during a collection.

2. **Core Functions:**

    * **Validation and Data Preparation:**
        * ```_get_customer_id```: Retrieves the customer ID using their identifier (e.g., ```CUIL``` or ```DNI```).
        * ```_get_credits_by_identifier```: Fetches credit IDs based on the identifier type and value.
        * ```_prepare_balance```: Prepares a credit balance by adjusting installments with recorded collections.

    * **Processing Collections:**
        * ```_process_regular_collections```: Handles standard collections by allocating payments to installments.
        * ```_process_early_payment```: Processes early payment amounts, optionally providing discounts or bonuses.
        * ```_process_penalty```: Creates a penalty record for outstanding amounts after a collection.
        * ```_solve_rounding```: Adjusts collection records to resolve rounding discrepancies.

    * **Massive Data Operations:**
        * ```read_collection_file```: Reads collection data from a CSV or Excel file and processes it into a structured DataFrame.
        * ```massive_collection```: Processes large-scale collections, grouping and summarizing data by identifier.
        * ```massive_early_collection```: Manages massive early cancellations, applying bonuses and handling adjustments.

    * **Advanced Collection Handling:**
        * ```charging```: Main function for processing collections, supporting regular payments, early settlements, and penalties.
        * ```collection_w_early_cancel```: Extends ```charging``` to include handling of early cancellations with bonuses.
        * ```reverse```: Handles the reversal of previously recorded collections.

3. **Database Interactions:**

    The module heavily interacts with the database using:

    * **Tables:**
        * ```credits```: Stores credit information.
        * ```customers```: Contains customer details.
        * ```installments```: Tracks installment payments.
        * ```collection```: Records collection data.
        * ```portfolio_purchases```: Contains details of purchased credit portfolios.
    * **Operations:**
        * Inserts, updates, and queries performed through SQLAlchemy and Pandas.

4. **Key Functionalities:**
    
    * **Processing Individual Collections:**
        * Validates the identifier and retrieves related credits and balances.
        * Allocates payments to installments, applying early payment bonuses or penalties if necessary.
        * Handles rounding errors to ensure financial precision.

    * **Massive Collection Handling:**
        * Supports processing large datasets of collection records from external files.
        * Groups records by specified identifiers (e.g., ```CUIL```, ```ID_Op```) and generates summaries.
        * Saves results to the database and provides error logs for unmatched or problematic entries.

    * **Early Settlements and Cancellations:**
        * Manages early payment discounts and cancellations, adjusting balances accordingly.
        * Applies bonuses for early cancellations to incentivize prompt payments.

    * **Reversal of Collections:**
        * Allows reversal of previously recorded collections, adjusting balances and reapplying outstanding amounts.

#### **Example Workflow:**

1. **Processing a Single Collection:**
    ```python
    collection = charging(TypeDataCollection.CUIL, 12345678901, 1000.0, id_supplier=1, save=True)
    ```

2. **Handling Massive Collections from a File:**
    ```python
    df, collection, error, penalties, installments = massive_collection(
        path="data/collections.xlsx",
        id_supplier=1,
        type_data=TypeDataCollection.CUIL,
        save=True
    )
    ```
3. **Reversing a Collection:**
    ```python
    reversed_collection = reverse(
        TypeDataCollection.ID_Op, 
        identifier=1001, 
        amount=500.0, 
        id_supplier=1, 
        save=True
    )
    ```

This module is critical for handling complex financial collection scenarios, ensuring accurate calculations, and maintaining data integrity across multiple database tables.

### Module Description: ```portfolio_manager.py```

The ```portfolio_manager.py``` module is designed to handle the acquisition, management, and processing of credit portfolios within the FinancialApp project. This includes validating suppliers and business plans, updating customer information, processing portfolio purchases, and generating associated credits, installments, and collections.

#### **Key Elements:**

1. **Validation Functions**

    * **```validate_supplier_and_business_plan```**:
        * Validates if a given supplier ID exists in the ```companies``` table and if a specified business plan is associated with that supplier.
        * Raises a ```ValueError``` if the supplier or business plan is invalid.

2. **File Handling**

    * **```load_file```**:
        * Loads data from CSV or Excel files into a Pandas DataFrame.
        * Ensures the file exists and handles common file formats.
    * **```read_data``**`:
        * Reads and preprocesses portfolio data from input files based on specific processing flags (```model```, ```iqua```, ```cfl```).
        * Handles various formats and structures of input data, such as customer and credit details.
        * Processes specific columns (e.g., names, addresses) to ensure consistency in the resulting DataFrame.

3. **Customer Management**

    * **```update_customers```**:
        * Updates the ```customers``` table in the database by:
            * Identifying new customers.
            * Updating records for existing customers.
        * Handles province IDs, gender normalization, and other customer attributes.
        * Can save changes directly to the database or process them locally.

4. *Portfolio Purchases*

    * **```add_portfolio_purchase```**:
        * Adds a new record to the ```portfolio_purchases``` table in the database.
        * Includes details such as the purchase date, supplier ID, interest rate (TNA), buyback conditions, resource involvement, and VAT applicability.

5. *Portfolio Processing*

    * **```process_portfolio```**:
        * Processes the portfolio by:
            * Generating new credits and installments based on the provided data.
            * Calculating missing or zero interest rates (```TEM_W_IVA```) using the ```numpy_financial.rate``` function.
            * Assigning unique IDs to new credits and installments.
            * Adjusting installments for VAT if necessary.
            * Creating collections for installments marked as "NO COMPRADA."
        * Saves the processed data to the database if the ```save``` flag is set.

6. **High-Level Portfolio Management**

    * **```portfolio_buyer```**:
        * High-level function for handling the entire portfolio acquisition process, which includes:
            1. Validating suppliers and business plans.
            2. Reading and preprocessing portfolio data from files.
            3. Updating customer records.
            4. Adding the portfolio purchase to the system.
            5. Generating new credits, installments, and collections.
        * Returns all processed data, including updated customer information, portfolio purchases, credits, installments, and collections.

#### **Key Functionalities:**

1. Supplier and Business Plan Validation:
    * Ensures that the portfolio purchase is associated with valid supplier and business plan IDs.
2. File Processing and Data Preprocessing:
    * Reads customer and credit data from input files (CSV or Excel).
    * Normalizes and structures data for consistent database integration.
3. Customer Record Management:
    * Handles the addition and updating of customer records, ensuring accurate and up-to-date information.
4. Portfolio Purchase Integration:
    * Records portfolio purchases in the database with detailed information, such as financial terms and conditions.
5. Credit and Installment Generation:
    * Generates new credits and their associated installments based on portfolio data.
    * Assigns unique IDs and calculates financial attributes like interest rates and installment values.
6. Collection Processing:
    * Handles collections related to purchased portfolios, including adjustments for VAT and installment conditions.

#### **Workflow Example:**

1. **Portfolio Purchase Processing:**

    ```python
    df, new_customers, pp, new_credits, installments, collections = portfolio_buyer(
        path="path/to/portfolio_file.xlsx",
        id_supplier=1,
        id_bp=2,
        tna=0.15,
        resource=True,
        iva=True,
        buyback=False,
        save=True
    )
    ```

    * Validates supplier and business plan.
    * Reads portfolio data from the specified file.
    * Updates customer records and adds the portfolio purchase.
    * Generates new credits, installments, and collections, saving them to the database.

3. **Validating a Supplier and Business Plan:**

    ```python
    validate_supplier_and_business_plan(id_supplier=1, id_bp=2)
    ```

4. **Updating Customers Locally:**
    ```python
    updated_customers = update_customers(df, date="2024-12-27", save=False)
    ```

This module is a critical component of FinancialApp, enabling seamless integration and processing of credit portfolios, while ensuring accurate and efficient management of related customer and financial data.

### Future Features

* Management of other types of investments.
* Debt management to finance investments.

## Contributions

If you wish to contribute, open a "pull request" or report a problem in the "issues" section.

## License

This project is under the MIT License.