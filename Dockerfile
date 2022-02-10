FROM python:3.7-stretch as base

# ensure to change the workdir to your repo name before pushing.
WORKDIR /Barbora 

COPY ./requirements.txt .

RUN apt-get update

RUN apt-get install default-jdk -y

RUN pip install --upgrade pip

RUN pip install --upgrade setuptools

RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "-m" , "unittest"]
