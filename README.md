# Airflow Example Project

This project sets up a flow with Apache airflow in Python. Airflow set up on a local server was used to create the environment variables needed to run the tests. If any anomalies are found, a message is sent to a Slack Webhook URL with the details.

## Assumptions

1. `snowflake_table_name` and `snowflake_last_update_column` will be passed in as parameters , brought in from Airflow variables. These, and the variables below, as shown in the image above, should be set up in advance.
2. The Snowflake connection is set up in the Airflow UI, under the `Admin/Connections` tab. Whatever the connection name is, in my case, `snowflake_connection_id`, should be saved in the `.env` file under the key `snowflake_connection_id`
3. snowflake credentials will be brought in from the local `.env` file
4. `snowflake table` and `column name` existance will not be tested - instead, error handling in the code will throw an exception if the query does not execute properly.
5. Slack notifications will be sent through a webhook; the webhook URL is stored as an **Airflow Variable** with the key `slack_webhook_url`
6. The DAG will be run daily (on the assumptions that the information is not needed real time, and that 1 day will be enough to follow up on detected anomalies)
7. The instructions below will decsribe how to install & run an `Airflow` server locally where a UI can be viewed of the DAG(s) set up for this system
8. The `login/pwd` for the local version of the servier are `admin/admin'
9. Anomaly tests - a list of all anomaly tests is shown in its own section
10. There will not be any alerts if all tests pass successfully. ** We will only alert on failure to reduce noise **
11. Project env variables should be stored in a `.env` file in the root folder; a template file is included with `.env_template`
  
## Data Quality Checks
This project includes the following data quality checks:

1.	`Null Values`: Flags rows with missing or null values.
2.	`Outliers`: Detects outliers using Z-scores for all numeric columns.
3.	`Value Range Check`: Flags values outside pre-defined acceptable ranges (e.g., temperature outside -50 to 50 degrees).
4.	`Duplicate Rows`: Detects any duplicate rows in the dataset.
5.	`Missing Datetime Intervals`: Detects missing data points for time-series data.

  
## Alerts/ Nofications
The code includes alerts:
* If all tests pass, a success message will be logged **only**, but no alerts will be sent.
  * **ERROR** Notifications will be sent through slack webhook.
    * Airflow Variable will have a value for `slack_webhook_url` key
    * NOTE: this is an easier and more simplistic way of setting up Slack notifications, but perhaps not the safest. With more time I would implement usage w/`WebClient` from the `Slack SDK`
  * A Jira Task ticket is set in JIRA though webhooks, as well. The following env variables are needed (please see the `.env_template` file for an example)
    * jira_api_key 
    * jira_url
    * jira_project_key
    * jira_id
  

## Installation

```bash
python3 -m venv env   
source env/bin/activate
#pip freeze > requirements.txt
pip install -r requirements.txt

# setting up airflow
airflow db init

# create airflow user
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# if you haven't already, copy over the template file
cp -n .env_template .env

# populate the .env variables as needed

#start the airflow webserver
airflow webserver --port 8080
```

