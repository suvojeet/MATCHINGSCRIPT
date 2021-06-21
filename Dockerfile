###########
# BUILDER #
###########

# pull official base image
FROM python:3.9.5-slim as builder

# set work directory
WORKDIR /usr/src

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install system dependencies
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y --no-install-recommends gcc g++

# lint
RUN pip install --upgrade pip
#RUN pip install flake8==3.9.1
COPY ./app ./app
#RUN flake8 --ignore=E501,F401 .

# install python dependencies
COPY ./requirements.txt .


RUN pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/wheels -r requirements.txt
#RUN pip install -r requirements.txt


#########
# FINAL #
#########

# pull official base image
FROM python:3.9.5-slim

# create directory for the app user
RUN mkdir -p /home/appuser

# create the group called match and app user
RUN addgroup --system matchapp && adduser --system --group appuser

# create the appropriate directories
ENV HOME=/home/appuser
ENV APP_HOME=/home/appuser/matchapp
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

# install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends netcat gcc g++
COPY --from=builder /usr/src/wheels /wheels
COPY --from=builder /usr/src/app .
RUN pip install --upgrade pip
RUN pip install --no-cache /wheels/*

# copy project
#COPY . $APP_HOME

# chown all the files to the app user
RUN chown -R appuser:matchapp $HOME

# change to the app user
USER appuser

EXPOSE 5000

CMD ["python3", "run.py"]