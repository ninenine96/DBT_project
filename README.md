# DBT Project showcasing the power of DBT-Snowflake shipped as a docker image

## Table of contents

1. [Prerequisites](#prerequisites)
1. [Technologies Used](#technologies-used)
1. [Project Overview](#project-overview)
1. [Learnings](#learnings)
1. [Dockerfile Code](#dockerfile)

## Prerequisites

-   dbt-core, dbt-snowflake running in a python environment.

    -   `pip install dbt-core`
    -   `pip install dbt-snowflake`
    -   Once DBT CLI is configured, run
    -   `dbt deps`
    -   to install any package dependencies for the dbt project.

-   For running the docker image, docker desktop is needed.

    -   `docker build -t <image-name> /`
    -   `dbt run -e cmd="<dbt command>" <image-name>`

## Technologies used

1. DBT CLI
2. Snowflake
3. Docker

## Project Overview

The basic idea of this project is using DBT in parallel with a data warehousing platform (snowflake) to create views and dimension tables for the given data. Using DBT is a no-brainer as it has many advantages such as built in support for [testing](https://docs.getdbt.com/docs/building-a-dbt-project/tests), [modularization](https://docs.getdbt.com/docs/building-a-dbt-project/building-models), [documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation). Once the project requirements have been met, converting it into a docker image is as easy as writing a docker configuration file and executing basic docker commands to integrate into a CI/CD workflow.

-   [Snowflake Documentation](https://docs.snowflake.com/en/user-guide-intro.html)
-   [DBT Documentation](https://docs.getdbt.com/docs/introduction)
-   [Docker Documentation](https://docs.docker.com/get-started/overview/)

## Learnings

1. DBT Fundamentals
2. Jinja templating
3. Abstracting SQL queries using Jinja
4. Power of DBT Macros and Packages
5. Creating and running docker images

## Dockerfile for DBT-Snowflake

```
FROM python:3.10.5
WORKDIR /
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
RUN dbt clean --project-dir /
RUN dbt deps --project-dir /
ENV cmd="dbt test"
ENV project_dir="--project-dir /"
CMD ["/bin/bash", "-c", "${cmd} ${project_dir}"]
```
