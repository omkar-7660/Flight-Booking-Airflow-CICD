Flight booking airflow CICD

we have created a workflow using the Apache-Airflow using GCP composer.
Here, we have automated the workflow by using the ci-cd with github.
When we push to the fearure branch or merge to main. Git-hub automatically triggers the actions.
in action it connect to the GCP then authenticket then upload the mentioned files to airflow comoser as well as the gcp buckets using gcloud and gsutil command. Then once the airflow-dag.py is uploaded to /dag folder we have to trigger it manually beacuse for now we kept the schedule_interval=None.
then we upload the .csv to corresponding folder. once the .csv available to the folder we have used sensor operator which sense/scan the file and triggers next task which spark execution . Once the spark execution is done we are moving those data frames to big query as table.    