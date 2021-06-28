### App
This app simulates an end-to-end custom data flow using API calls to retrieve geographical and
financial data, for the purpose of downstream analytics. 

### feature
- the solution consists of two separate components which need to be run sequentially in order to produce viable result.
  Subsequently, the pipelining of the data starts from the ingestion phase, before going through minimal
  cleansing to eventually the load stage. The ETL code currently resides in base/main.py.
  It should be noted that in an production environment, each phase i.e. ingestion/cleansing/load would have its
  own separate process.  

- Used both pyspark api and pandas library to complete various operations/tasks in the solution.

- includes logging

### Assumptions
- the solution was meant to be treated as a data flow which could be pipelined using any orchestrator (with of course refactoring and scaling). 

- Unit test metrics modified for the queries to be answerable for aggregate based operations.

### Setup
- Clone the repo
- Create and activate virtual environment for project (Use either virtualevn (Run python3 -m venv <name_of_virtualenv>) or virtualenvwrapper- depending on preference)
- pip install -r requirements.txt (Install all the requirements)
- cd to {project_dir}/base
- run "python main.py" to execute ETL job
- run "python analysis.py" to create derived spark views

###tests
- uses pytest framework
#### Setup
- Ensure environment has been configured to run pytest tests.
- cd to {project_dir}/tests
- run "python test_process.py" to process test
- run "python test_queries.py" to execute all functional/aggregate tests
- logging included

