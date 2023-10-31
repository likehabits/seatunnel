FROM maven:3.8.1-openjdk as build

WORKDIR /opt

ADD . /opt/

RUN ARCH=`arch | sed s/arm64/aarch_64/ | sed s/aarch64/aarch_64/ | sed s/amd64/x86_64/` \
  && mvn clean install -DskipTests -Dcheckstyle.skip -Dmaven.javadoc.skip=true -Dos.detected.classifier=linux-${ARCH}

FROM openjdk:8-jre

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update \
  && apt -y install tzdata \
  && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

ADD seatunnel-dist/target/apache-seatunnel-*-SNAPSHOT-bin.tar.gz /seatunnel
RUN mv /seatunnel/apache-seatunnel-2.3.3-SNAPSHOT /seatunnel/apache-seatunnel

ENTRYPOINT ["/bin/sh","/seatunnel/apache-seatunnel/bin/seatunnel-cluster.sh","-d"]