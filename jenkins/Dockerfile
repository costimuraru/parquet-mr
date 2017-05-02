# Set the base image to centos
FROM centos:latest

# File Author / Maintainer
MAINTAINER cmuraru

################## BEGIN INSTALLATION ######################

################## BASE ####################################

RUN yum -y groupinstall 'Development Tools'
RUN yum -y install wget tar unzip gzip git rpm-build nc gcc tar unzip gzip make zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libyaml libtool automake autoconf gcc-c++ boost-devel flex bison glib-devel xorg-x11-devel libssl

################## JAVA ####################################

RUN rpm -qa | grep -q jdk || wget --no-cookies --no-check-certificate  --header "Cookie: oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u45-b14/jdk-8u45-linux-x64.rpm" -O "/opt/jdk-8-linux-x64.rpm" && \
    echo "export JAVA_HOME=/usr/java/default" > /etc/profile.d/java_home.sh && \
    rpm -qa | grep -q jdk || rpm -Uvh "/opt/jdk-8-linux-x64.rpm" && \
    ln -fs /usr/java/default/bin/jps /usr/bin/jps && \
    rm -f /opt/jdk-8-linux-x64.rpm
ENV JAVA_HOME /usr/java/default
ENV PATH $JAVA_HOME/bin:$PATH

################## MAVEN ##################################

# Download Maven
RUN wget --no-verbose -O /tmp/apache-maven-3.2.2.tar.gz http://archive.apache.org/dist/maven/maven-3/3.2.2/binaries/apache-maven-3.2.2-bin.tar.gz
# install maven
RUN tar xzf /tmp/apache-maven-3.2.2.tar.gz -C /opt/
RUN ln -s /opt/apache-maven-3.2.2 /opt/maven
RUN ln -s /opt/maven/bin/mvn /usr/local/bin
RUN rm -f /tmp/apache-maven-3.2.2.tar.gz
ENV MAVEN_HOME /opt/maven

##################### ANT #####################

# Update list of packages and install Ant
RUN yum -y install ant

##################### PARQUET #####################

RUN mkdir -p /usr/local/protobuf_install && \
    cd /usr/local/protobuf_install && \
    wget https://github.com/google/protobuf/archive/v3.2.0.tar.gz -O protobuf-3.2.0.tar.gz && \
    tar xzf protobuf-3.2.0.tar.gz && \
    cd protobuf-3.2.0 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make check && \
    make install && \
    ldconfig && \
    protoc --version



##################### INSTALLATION END #####################

ADD files/settings.xml /root/.m2/settings.xml

ADD files/settings.xml /tmp/settings.xml

RUN cat /root/.m2/settings.xml

CMD /bin/bash

