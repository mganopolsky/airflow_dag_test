from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import pandas as pd
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
import requests
import json
import os
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import logging

# Load environment variables
load_dotenv()

# Function to safely get variables with defaults
def get_airflow_var(var_name, default_value=None):
    try:
        return Variable.get(var_name)
    except KeyError:
        if default_value is not None:
            return default_value
        raise AirflowException(f"Required variable {var_name} not set in Airflow")

# Access environment variables and Airflow variables with defaults
try:
    snowflake_connection_id = os.getenv('snowflake_connection_id', 'snowflake_default')
    jira_token = os.getenv('jira_api_key')
    jira_url = os.getenv('jira_url')
    jira_project_key = os.getenv('jira_project_key')
    jira_user = os.getenv('jira_id')
    snowflake_table_name = get_airflow_var('snowflake_table_name')
    snowflake_last_update_column = get_airflow_var('snowflake_last_update_column', 'updated_at')
    date_range_start = get_airflow_var('date_range_start', '2024-01-01')
    date_range_end = get_airflow_var('date_range_end', '2024-12-31')
    slack_webhook_url = get_airflow_var('slack_webhook_url')
    test_for_missing_data_column_name = get_airflow_var('potential_null_column_name')
    value_range_bottom_limit = float(get_airflow_var('value_range_bottom_limit', '0'))
    value_range_top_limit = float(get_airflow_var('value_range_top_limit', '1000'))
    value_range_column_name = get_airflow_var('value_range_column_name')
    z_scaler_outlier_column_name = get_airflow_var('z_scaler_outlier_column_name')

except Exception as e:    
    logging.error(f"Failed to load required variables: {str(e)}")
    is_dag_enabled = False
else:
    is_dag_enabled = True

def create_jira_task(error_summary:str, issue_type: str='task'):
    
    # Jira API endpoint for creating an issue    
    url = f"{jira_url}/rest/api/3/issue"

    # Payload with issue details
    payload = {
        "fields": {
            "project": {
                "key": project_key  # Your project key in Jira, e.g., "PROJ"
            },
            "summary": error_summary,
            "description": error_summary,
            "issuetype": {
                "name": issue_type  # Issue type, like "Task" or "Bug"
            }
        }
    }

    # Headers for the request
    headers = {
        "Content-Type": "application/json"
    }

    # Make the POST request to create the Jira issue
    response = requests.post(
        url,
        headers=headers,
        auth=HTTPBasicAuth(jira_user, jira_token),
        data=json.dumps(payload)
    )

    # Check the response status
    if response.status_code == 201:
        print("Task created successfully in Jira.")
        print("Jira Task URL:", response.json().get("self"))
    else:
        print(f"Failed to create Jira task. Status code: {response.status_code}, Response: {response.text}")

# Function to log success
def log_success_message():
    logging.info(f"✅ All tasks completed successfully")

# Function to send Slack message
def send_slack_message(message):
    try:
        response = requests.post(
            slack_webhook_url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps({"text": message}),
            timeout=10
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to send Slack message: {str(e)}")

# Function to send failure alert
def send_failure_alert(context):
    task_instance = context['task_instance']
    task_name = task_instance.task_id
    error_message = context.get('exception')
    
    message = f"❌ Task {task_name} failed with error: {error_message}"
    send_slack_message(message)

default_args = {
    'owner': 'marina',
    'start_date': days_ago(10),
    'retries': 1,
    'on_failure_callback': send_failure_alert
}

def fetch_data_from_snowflake(**context):
    hook = SnowflakeHook(snowflake_conn_id=snowflake_connection_id)
    query = f"""
    SELECT * 
    FROM {snowflake_table_name} 
    WHERE {snowflake_last_update_column} >= DATEADD(day, -7, CURRENT_DATE)
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:        
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        
        stats = {
            'row_count': len(df),
            'null_counts': df[test_for_missing_data_column_name].isnull().sum(),
            'value_range': {
                'min': df[value_range_column_name].min(),
                'max': df[value_range_column_name].max()
            },
            'z_scores': calculate_z_scores(df[z_scaler_outlier_column_name]),
            'duplicate_count': df.duplicated().sum()
        }
        
        context['task_instance'].xcom_push(key='data_stats', value=stats)
        return "Data fetch successful"
        
    except Exception as e:
        logging.error(e.with_traceback)
        raise AirflowException(f"Failed to fetch data: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def calculate_z_scores(series):
    scaler = StandardScaler()
    z_scores = scaler.fit_transform(series.values.reshape(-1, 1))
    return z_scores.flatten().tolist()

def test_null_values(**context):
    stats = context['task_instance'].xcom_pull(key='data_stats')
    if stats['null_counts'] > 0:
        raise AirflowException(f"Found {stats['null_counts']} null values")
    return "null_check_passed"

def test_outliers_z_score(**context):
    stats = context['task_instance'].xcom_pull(key='data_stats')
    z_scores = stats['z_scores']
    outliers = [z for z in z_scores if abs(z) > 3]
    if outliers:
        raise AirflowException(f"Found {len(outliers)} outliers")
    return "outlier_check_passed"

def test_value_range(**context):
    stats = context['task_instance'].xcom_pull(key='data_stats')
    value_range = stats['value_range']
    if value_range['min'] < value_range_bottom_limit or value_range['max'] > value_range_top_limit:
        raise AirflowException(f"Values out of range: min={value_range['min']}, max={value_range['max']}")
    return "range_check_passed"

def test_duplicate_rows(**context):
    stats = context['task_instance'].xcom_pull(key='data_stats')
    if stats['duplicate_count'] > 0:
        raise AirflowException(f"Found {stats['duplicate_count']} duplicate rows")
    return "duplicate_check_passed"

# Determine whether to go to success or failure path
def determine_success(**context):
    task_instance = context['task_instance']
    test_tasks = [
        'test_null_values',
        'test_outliers_z_score',
        'test_value_range',
        'test_duplicate_rows'
    ]
    
    failed_tasks = []
    for task in test_tasks:
        try:
            result = task_instance.xcom_pull(task_ids=task)
            if not result.endswith('_passed'):
                failed_tasks.append(task)
        except Exception:
            failed_tasks.append(task)
    
    if failed_tasks:
        return 'send_failure_message'
    return 'success_tasks_done_task'

with DAG(
    'snowflake_anomaly_detection',
    default_args=default_args,
    description='DAG to detect anomalies from Snowflake data and send alerts',
    schedule_interval='@daily',
    catchup=False,  # Prevent backfilling
    is_paused_upon_creation=True,  # Start in paused state
) as dag:
    if not is_dag_enabled:
        raise AirflowException("DAG is disabled due to missing required variables")

    # Define tasks
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_snowflake,
        provide_context=True,
        on_failure_callback=send_failure_alert
    )

    test_null_values_task = PythonOperator(
        task_id='test_null_values',
        python_callable=test_null_values,
        provide_context=True,
    )

    test_outliers_z_score_task = PythonOperator(
        task_id='test_outliers_z_score',
        python_callable=test_outliers_z_score,
        provide_context=True,
    )

    test_value_range_task = PythonOperator(
        task_id='test_value_range',
        python_callable=test_value_range,
        provide_context=True,
    )

    test_duplicate_rows_task = PythonOperator(
        task_id='test_duplicate_rows',
        python_callable=test_duplicate_rows,
        provide_context=True,
    )

    check_results_task = BranchPythonOperator(
        task_id='check_results',
        python_callable=determine_success,
        provide_context=True,
    )

    # Success task
    log_success_task = PythonOperator(
        task_id='success_tasks_done_task',
        python_callable=log_success_message
    )

    # Failure task
    send_failure_message_task = PythonOperator(
        task_id='send_failure_message',
        python_callable=send_slack_message,
        op_kwargs={'message': '❌ Some data quality checks failed. Check the logs for details.'},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Failure task
    create_jira_item_task = PythonOperator(
        task_id='create_jira_item_task',
        python_callable=create_jira_task,
        op_kwargs={'error_summary': '❌ Some data quality checks failed. Check the logs for details.'},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Set up the task dependencies
    fetch_data_task >> [
        test_null_values_task,
        test_outliers_z_score_task,
        test_value_range_task,
        test_duplicate_rows_task
    ] >> check_results_task

    # Branching logic for success/failure
    check_results_task >> [log_success_task, send_failure_message_task, create_jira_item_task]