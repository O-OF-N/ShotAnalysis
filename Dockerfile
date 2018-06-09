FROM ubuntu:latest

RUN apt-get update
RUN apt-get -y install software-properties-common
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update

# Install Python
RUN apt-get install -y python3 python3-dev python3-pip python3-virtualenv python3-setuptools && \
  pip3 install --upgrade pip && \
  rm -rf /var/lib/apt/lists/*

RUN apt-get update

# Install Java
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

RUN apt-get update


# Environment variables
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV PYSPARK_PYTHON /usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON /usr/bin/python3

# Define default command.
CMD ["bash"]

#Set workdir
WORKDIR /var/www

#Install dependencies
ADD ./requirements.txt /var/www/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

#Copy resources
COPY ./src /var/www/src
COPY ./test /var/www/test
COPY ./integration-test /var/www/integration-test
COPY ./driver.py /var/www/driver.py
COPY ./setup.py /var/www/setup.py
COPY ./resources /var/www/resources

#Package
RUN python3 setup.py sdist


#Unit test
RUN py.test -s -v

#Define entry point
#ENTRYPOINT [ "spark-submit","--py-files","dist/Shot-Analysis-0.1.dev0.tar.gz","driver.py" ]

