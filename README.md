# Airflow-Project

## Test the tasks you are working on

Search the scheduler container

```sh
docker-compose ps
```

Open container console

```sh
docker exec -it [scheduler_container_name] /bin/bash
```

Execute task
```sh
airflow tasks test [dag_name] [task_name] 2023-11-13
```