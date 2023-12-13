## Run

Execute commands:
1. docker compose build airflow-webserver
2. docker compose up

## Details

1. Didn't use docker image extension given in the task description. Because the common way described on Airflow website is to build another image.  
And, then, change some configurations in common part of servies in the begining of docker-compose.yaml.