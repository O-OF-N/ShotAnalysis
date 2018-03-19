FROM ubuntu:latest

RUN apt-get update
RUN apt-get -y install software-properties-common python-software-properties
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update

# Install Python
RUN apt-get install -y python3 python3-dev python3-pip python3-virtualenv python3-setuptools && \
  #pip3 install --upgrade pip \
  rm -rf /var/lib/apt/lists/*

RUN apt-get update

# Install Java
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer


# Define JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# Define default command.
CMD ["bash"]

WORKDIR /var/www

ADD ./requirements.txt requirements.txt
COPY . /var/www
RUN pip3 install --no-cache-dir -r requirements.txt
#RUN py.test -s -v
RUN python setup.py sdist
ENTRYPOINT [ "spark-submit","--py-files","dist/Shot-Analysis-0.1.dev0.tar.gz","driver.py" ]

