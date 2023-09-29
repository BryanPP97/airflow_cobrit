FROM apache/airflow:2.7.1-python3.11
COPY requirements.txt /
RUN pip install --upgrade pip
RUN pip install python-dotenv yagmail nltk pyodbc
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update
RUN apt-get install -y wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
USER airflow