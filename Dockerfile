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
