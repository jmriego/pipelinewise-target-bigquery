import logging
from google.api_core.future import polling
from google.cloud import bigquery
from google.cloud.bigquery import retry as bq_retry
from google.api_core.retry import if_exception_type, Retry

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


## 1. Retry at the Google API level (Deadline = 30 seconds)
# NOTE: We can isolate the exception types at a later point - Just use `Exception` for now
@Retry(predicate=if_exception_type(Exception), deadline=30)
def query(client, query, params=[]) -> bigquery.job.query.QueryJob:
    def to_query_parameter(value):
        if isinstance(value, int):
            value_type = "INT64"
        elif isinstance(value, float):
            value_type = "NUMERIC"
        elif isinstance(value, bool):
            value_type = "BOOL"
        else:
            value_type = "STRING"
        return bigquery.ScalarQueryParameter(None, value_type, value)

    job_config = bigquery.QueryJobConfig()
    query_params = [to_query_parameter(p) for p in params]
    job_config.query_parameters = query_params

    queries = []
    if type(query) is list:
        queries.extend(query)
    else:
        queries = [query]

    logging.info("TARGET_BIGQUERY - Running query: {}".format(query))

    ## 2. Retry at the Bigquery API level
    query_job = client.query(';\n'.join(queries), job_config=job_config, retry=bq_retry.DEFAULT_RETRY)
    query_job._retry = polling.DEFAULT_RETRY
    query_job.result()
    ##
    return query_job


# Init Client
project_id = 'sandbox-278017'
location = 'US'
client = bigquery.Client(project=project_id, location=location)

# (i) Test Correct BQ Query string
correct_query_string = "SELECT * FROM `sandbox-278017.hermes_export.admin_audit` LIMIT 10"

# POST Query
query_result = query(client, correct_query_string)
print(type(query_result))

# Print Results
for row in query_result.result():
    print(row)

# (ii) Test incorrect BQ Query string
incorrect_query_string = "SELECT * FROM `sandbox-278017.hermes_export.table_that_does_not_exist` LIMIT 10"

failed_query_result = query(client, incorrect_query_string)
