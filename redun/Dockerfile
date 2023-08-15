FROM python:3.10
RUN apt-get update -y && apt-get install -y libpq-dev gcc   graphviz graphviz-dev
RUN pip install 'redun[viz]' 'redun[postgres]' 'redun'
RUN apt-get install -y postgresql-client
