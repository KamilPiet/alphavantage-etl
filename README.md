# Alpha Vantage ETL

[_Here_](https://kamilpiet.github.io/)
is a weekly updated demo price report.

## General information

This is a simple ETL pipeline written in Python that pulls data from the [Alpha Vantage](https://www.alphavantage.co/) API,  
loads it into a PostgreSQL database and visualizes it using Plotly and Datapane.  
It can run as an Apache Airflow DAG, inside a Docker container as an AWS Lambda function or on any machine runnig Linux.

The resulting price report provides an overview of the recent and historical daily exchange rate of USD  
against selected currency, as well as the recent and historical daily price of the selected security  
in both USD and the selected currency. This report can be helpful in analyzing price trends of securities  
in currencies other than USD.

## Technologies used

- Python 3.9
  - [pandas](https://pandas.pydata.org/)
  - [Plotly](https://plotly.com/graphing-libraries/)
  - [Datapane](https://datapane.com/)
- PostgreSQL
- [Apache Airflow](https://airflow.apache.org/)
- [AWS Lambda](https://aws.amazon.com/lambda/)
- [Docker](https://www.docker.com/)
- GitHub Pages


## Setup

### Apache Airflow

#### Prerequisites

- Apache Airflow installed and running
- PostgreSQL database set up
- GitHub Pages repository created

#### Setup

- Clone this repository and install required modules:  
```
git clone https://github.com/KamilPiet/alphavantage-etl.git
pip3 install -r ./alphavantage-etl/airflow/requirements.txt
```

- Create a new postgres connection named `postgres_alphavantage` with database details:
```
airflow connections add 'postgres_alphavantage' \
    --conn-uri 'Postgres://<login>:<password>@<host>:<port>/<schema>'
```

- Create following environment variables:  
(You can add following lines to ~/.profile or ~/.bash_profile to set these variables permanently)
```
export ALPHAVANTAGE_API_KEY=<your Alpha Vantage API key*>
export AV_ETL_GITHUB_TOKEN=<your GitHub access token**>
export AV_ETL_GIT_USERNAME=<your GitHub username>
export AV_ETL_GIT_EMAIL=<your GitHub email address>
export AV_ETL_REMOTE_REPO=<URL to your GitHub Pages repository>
export AV_ETL_WORKING_DIR_PATH=<path to the directory where the price report will be saved and local git repo created>
 ```
*An API key can be obtained [_here_](https://www.alphavantage.co/support/#api-key)  
**The token should include at least the following scopes: repo, read:user, and user:email
- Move `av_etl.py`, `constants.py`, `data_viz.py` and `to_github_pages.py` into `$AIRFLOW_HOME/plugins`
- Move `airflow/av_etl_dag.py` into `$AIRFLOW_HOME/dags`
- Unpause the created DAG (`alphavantage_etl_dag`):
```
mv -t $AIRFLOW_HOME/plugins ./alphavantage-etl/av_etl.py ./alphavantage-etl/constants.py ./alphavantage-etl/data_viz.py ./alphavantage-etl/to_github_pages  
mv -t $AIRFLOW_HOME/dags ./alphavantage-etl/airflow/av_etl_dag.py
airflow dags unpause alphavantage_etl_dag
```

### AWS Lambda

#### Prerequisites

- Docker installed and running 
- AWS CLI installed and configured 
- PostgreSQL database set up 
- GitHub Pages repository created

#### Setup
- Clone this repository and build a Docker container image:  
```
git clone https://github.com/KamilPiet/alphavantage-etl.git
docker build -t <image_name> -f ./alphavantage-etl/aws-lambda/Dockerfile ./alphavantage-etl
```
- Push the image to the AWS Elastic Container Registry (follow steps 2-4 from
[_this_](https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html#cli-authenticate-registry)
guide,  
please note that the name of the repository must match the name of the built image)
- Create an AWS Lambda function:
  - On the Lambda console page click `Create function`
  - Select `Container image` option
  - Click `Browse images`
  - Select the appropriate ECR repository and image
  - Click `Create function`
- Create following environment variables in the Labmda function configuration tab:  
  - `ALPHAVANTAGE_API_KEY` - your Alpha Vantage API key (it can be obtained 
[_here_](https://www.alphavantage.co/support/#api-key))
  - `AV_ETL_GITHUB_TOKEN` - your GitHub access token*
  - `AV_ETL_GIT_USERNAME` - your GitHub username
  - `AV_ETL_GIT_EMAIL` - your GitHub email address
  - `AV_ETL_REMOTE_REPO` - URL to your GitHub Pages repository
  - `AV_ETL_DB_LOGIN` - database login
  - `AV_ETL_DB_PASSWORD` - database password
  - `AV_ETL_DB_HOST` - database host name
  - `AV_ETL_DB_PORT` - database port
  - `AV_ETL_DB_NAME` - databese name  

*The token should include at least the following scopes: repo, read:user, and user:email
- In the Lambda function configuration tab, increase the timeout value to at least 1 minute and 30 seconds  
and the memory limit to 512 MB

## Usage

### General information

- You can select a different security and a different currency to be presented in the price report  
by changing the values of the global variables `SYMBOL` and `CURRENCY` in `constants.py`  
(if the pipeline is to run on AWS Lambda, the change must be made before building the docker image)  
([_here_](https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo)
is a list of all securities available)


### Apache Airflow

- The DAG will run as scheduled in `airflow/av_etl_dag.py`  
(by default it will run at 12:00 PM UTC every Sunday)
- You can change the schedule by changing the value of the `schedule_interval` parameter when creating the DAG object in
`airflow/av_etl_dag.py`
- Alternatively, you can trigger the DAG manually:
```
airflow dags trigger alphavantage_etl_dag
```

### AWS Lambda

- [_Here_](https://docs.aws.amazon.com/lambda/latest/dg/lambda-invocation.html)
is a guide on how to invoke a Lambda function
- The simplest method to invoke a function is to run a test

## Planned improvements  

- Add an option to connect to GitHub via SSH
- Add a configuration file
- Make publishing a report on GitHub Pages optional
