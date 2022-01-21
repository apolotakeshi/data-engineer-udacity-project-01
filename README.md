
# Summary of the Project - Project: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
This projects deliver an ETL pipeline using Python.
- Fact and Dimension Tables has been built
- A Star Schema has been designed
- Files were inserted from the `data` folders
- Postgres and SQL has also been used

# How to run the Python Scripts

## Prerequisites
    Make sure you have already installed both (Docker Engine)[https://docs.docker.com/get-docker/] and (Docker Compose)[https://docs.docker.com/compose/install/]. Everything else you might need will be provided by Docker images.

## Setup and Files explanation

Instructions
------------

1. You are advised to have a GitHub account to clone this process. [Register for free](http://www.github.com).

2. After you login, click on `"fork"` - top right corner of the page. 

3. Now that you have this repository you can start to work on it.

4. run:
    `docker-compose up`

5. click on the link: [http://localhost:8888/?token=docker](http://localhost:8888/?token=docker)

6. In JupyterLab, open a Terminal and run:
    `make etl`

7. Go to `notebooks/test.ipynb` and explore

8. When you are done, go back to your machine and run:
    `docker-compose down --volumes --remove-orphans`

## Files explanation

```bash
├── data                              contains the raw_data
│   ├── log_data
│   │   └── 2018
│   │       └── 11
│   │           ├── 2018-11-01-events.json
│   │           ├── ...
│   │           └── 2018-11-30-events.json
│   └── song_data
│       └── A
│           ├── A
│           │   ├── A
│           │   │   ├── TRAAAAW128F429D538.json
│           │   │   ├── ...
│           │   │   └── TRAAAVO128F93133D4.json
│           │   ├── B
│           │   │   ├── ...
│           │   │   └── TRAABYW128F4244559.json
│           │   └── C
│           │       ├── ...
│           │       └── TRAACZK128F4243829.json
│           └── B
│               ├── A
│               │   ├── ...
│               │   └── TRABAZH128F930419A.json
│               ├── ...
│               │
│               └── C
│                   ├── TRABCAJ12903CDFCC2.json
│                   └── TRABCYE128F934CE1D.json
│
├── docker-compose.yml                prepares the environment
├── Dockerfile                        adapt the jupyter image
├── etl.py                            main process of the ETL
├── Makefile                          to save some time later on
├── notebooks                         for exploratory analysis
│   ├── etl.ipynb                     Replicates the etl pipeline step by step
│   └── test.ipynb                    Allows for query tests 
│
├── README.md                         Explains how to use this project
├── requirements.txt                  Incremental on top of jupyterLab image in usage
├── scripts                           Some scripts that can be used in multiple projects
│   └── templating.py
└── table_creation                    helds the deliverables for the step table_creation
    ├── create_tables.py
    └── sql_queries.py
```

