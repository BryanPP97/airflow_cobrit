FROM apache/airflow:latest-python3.9

COPY requirements.txt /
RUN pip install --upgrade pip && \
    pip install python-dotenv yagmail nltk pyodbc && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y wget gosu && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /google-chrome-stable_current_amd64.deb

USER airflow
