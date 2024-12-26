# FinancialApp

**FinancialApp** is an application designed to manage a credit portfolio. The platform enables you to oversee both credits granted by the company and those acquired from third parties. Future versions will include functionalities for managing additional investment types and handling debt to finance these investments.

## Project Structure

The project structure is organized as follows:

```
FinancialApp/ 
├── app/
│   ├── __init__.py
│   └── modules/
│       └── __init__.py
├── docs/
│   ├── requirements.txt 
│   └── AppStructure.sql
├── logs/
│   ├── log.py
│   └── run_log.py
├── inputs
├── notebooks/
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

* **run_log.py:** Starts the log management.
* **Start_DataBases.ipynb:** Executes data analysis and queries related to the credit database.

## Future Features

* Management of other types of investments.
* Debt management to finance investments.

## Contributions

If you wish to contribute, open a "pull request" or report a problem in the "issues" section.

## License

This project is under the MIT License.
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
│   └── Start_DataBases.ipynb
├── fa_env/
├── .gitignore
└── README.md
```

### Folder Description

- **app/:** Contains the main source code of the application, organized into modules necessary for its functionality.
- **docs/:** Includes documents related to client data and the structure of the credit portfolio.
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

* **run_log.py:** Starts the log management.
* **Start_DataBases.ipynb:** Executes data analysis and queries related to the credit database.

## Future Features

* Management of other types of investments.
* Debt management to finance investments.

## Contributions

If you wish to contribute, open a "pull request" or report a problem in the "issues" section.

## License

This project is under the MIT License.