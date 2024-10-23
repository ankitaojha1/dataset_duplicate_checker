# Dataset Duplicate Checker with PySpark

This project provides a **duplicate checking tool** implemented in **PySpark**, along with unit tests to validate its functionality. The tool is designed to detect duplicates in a PySpark DataFrame based on specified columns and is packaged to run seamlessly in a **Docker container**.

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
├── dataset_duplicate_checker
│    ├── __init__.py                     # Initialization file for the module
│    ├── duplicate_checker.py            # Contains the PySparkDuplicateChecker class
│    └── tests
│        └── test_duplicate_checker.py   # Unit tests for the PySparkDuplicateChecker class
├── README.md
├── requirements.txt                     # Python dependencies (PySpark, pytest, pre-commit, etc.)
├── .pre-commit-config.yaml              # Pre-commit hook configuration
└── .gitignore                           # Optional: Ignore unnecessary files (e.g., virtualenv, pycache, etc.)
```


- **`duplicate_checker/checker.py`**: Main file that contains the `PySparkDuplicateChecker` class to check for duplicates in a PySpark DataFrame.
- **`duplicate_checker/tests/test_checker.py`**: Unit tests to validate the functionality of the duplicate checker.
- **`Dockerfile`**: Defines the Docker environment for the project.

---

## Requirements

To run this project, you'll need:

1. **Python 3.10+** (for local development)
2. **PySpark**: Installed either locally or via Docker.
3. **pytest**: For running unit tests.
4. **Docker** (if using Docker for deployment).

## Setup

### Local Environment

To run the project locally, follow these steps:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/your-repo/duplicate-checker.git
    cd duplicate-checker
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

This will run the default command in the `Dockerfile` (executing the tests).
