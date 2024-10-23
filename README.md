# Dataset Duplicate Checker with PySpark

This project provides a **duplicate checking tool** implemented in **PySpark**, along with pytests to validate its functionality. The tool is designed to detect duplicates in a PySpark/Pandas DataFrame based on specified columns and is packaged to run seamlessly in a **Docker container**.

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
  - [Local Environment](#local-environment)
  - [Docker Setup](#docker-setup)
- [Running the Duplicate Checker](#running-the-duplicate-checker)
- [Running Tests](#running-tests)
  - [Local Tests](#local-tests)
  - [Docker Tests](#docker-tests)
- [Edge Cases](#edge-cases)
- [Pre-Commit Hooks](#pre-commit-hooks)
- [Contributing](#contributing)

---

## Project Structure
```
.
├── Dockerfile
├── src
│    ├── __init__.py                     # Initialization file for the module
│    ├── duplicate_checker.py            # Contains the DuplicateChecker class
├── tests
│    ├── __init__.py                     
│    ├── test_duplicate_checker.py       # Pytests for the DuplicateChecker class
├── examples
│    ├── example_usage.py                # Examples for the DuplicateChecker class
├── __init__.py
├── README.md                            # Readme file for the project
├── requirements.txt                     # Python dependencies (PySpark, pytest, pre-commit, etc.)
├── .pre-commit-config.yaml              # Pre-commit hook configuration
└── .gitignore                           # Optional: Ignore unnecessary files (e.g., virtualenv, pycache, etc.)
```


- **`src/duplicate_checker.py`**: Main file that contains the `DuplicateChecker` class to check for duplicates in a PySpark/Pandas DataFrame.
- **`tests/test_duplicate_checker.py`**: Pytests to validate the functionality of the duplicate checker.
- **`Dockerfile`**: Defines the Docker environment for the project.

---

## Requirements

To run this project, you'll need:

1. **Python 3.8+** (for local development)
2. **PySpark**: Installed either locally or via Docker.
3. **pytest**: For running tests.
4. **Docker** (if using Docker for deployment).

## Setup

### Local Environment

To run the project locally, follow these steps:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/ankitaojha1/dataset_duplicate_checker.git
    cd dataset_duplicate_checker
    ```

2. **Install dependencies**:
    Create a virtual environment and install the required packages.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3. **PySpark Setup**:
    Make sure you have **PySpark** set up on your local machine. You can install it via `pip`:
    ```bash
    pip install pyspark
    ```

### Docker Setup

To run the project using Docker, make sure you have **Docker** installed and follow these steps:

1. **Build the Docker image**:
    ```bash
    docker build -t dataset_duplicate_checker .
    ```

2. **Run the container**:
    ```bash
    docker run dataset_duplicate_checker
    ```

This will run the default command in the `Dockerfile` (executing the examples from examples/example_usage.py).

### Running the Duplicate Checker

The entrypoint in the Dockerfile is configured to run examples of what the project is supposed to do, i.e., to detect duplicates in a DataFrame based on specified columns.

You can also use the `DuplicateChecker` class separately, to check for duplicates in a DataFrame. Here is an example of how to run it:
```bash
    from pyspark.sql import SparkSession
    from dataset_duplicate_checker.src.duplicate_checker import DuplicateChecker

    spark = SparkSession.builder.appName("DuplicateCheckerApp").getOrCreate()
    data = [
    ('A', 'x'),
    ('A', 'x'),
    ('B', 'y'),
    ('C', 'z')
    ]

    df = spark.createDataFrame(data, ['col_1', 'col_2'])

    # Check for duplicates based on 'col_1'
    result = DuplicateChecker.check_duplicates(df, ['col_1'])

    # Show results
    print("Duplicate Count:", result['count'])
    result['samples'].show()

    spark.stop()
```
**Expected Output**:
- Duplicate count based on the column(s) passed.
- DataFrame of duplicate rows, if any.

### Running Tests
**Local Tests**

To run the tests in your local environment using pytest, execute:
```bash
    pytest duplicate_checker/tests/test_duplicate_checker.py
```
**Docker Tests**

Note that the last line in the Dockerfile is configured to run the tests inside Docker, you can uncomment the below line in the Docker file, then build and run the docker image.
```bash
    #CMD ["pytest", "tests/test_duplicate_checker.py"]
```

### Edge Cases
The DuplicateChecker class handles the following edge cases:
- **Empty DataFrame**: Raises a ValueError if an empty DataFrame is passed.
- **Non-existent columns**: Raises a ValueError if the column(s) provided do not exist in the DataFrame.
- **Empty column list**: Raises a ValueError if no columns are provided for checking duplicates.

### Pre-Commit Hooks
This project uses `pre-commit` hooks to ensure code quality and consistency. Hooks are used to enforce code formatting, linting, and other checks before commits are made. The following tools are integrated via pre-commit:
- **Black**: For automatic code formatting.
- **Flake8**: For style guide enforcement and linting.
- **Pylint**: For static code analysis and code quality checks.
- **Trailing Whitespace and End of File Fixer**: For fixing common whitespace issues.
- **Merge Conflict Checker**: To detect unresolved merge conflicts in files.

### Contributing
Please follow the steps below for contributing:
- Create a new branch: git checkout -b my-feature-branch.
- Make your changes and commit them: git commit -m 'Add new feature'.
- Push to the branch: git push origin my-feature-branch.
- Submit a pull request.

### License
This project is licensed under the MIT License.