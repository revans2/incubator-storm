#!/bin/bash

# Error on anything that goes wrong.
set -e

if [[ "${ystorm__remote_logging}" == [Tt][Rr][Uu][Ee] ]]
then
  SOCKET_APPENDER_REF_ELEM='<appender-ref ref="SOCKET"/>'
else
  SOCKET_APPENDER_REF_ELEM='<!-- Use yinst set ystorm.remote_logging=true to enable remote logging. -->'
fi


cat <<XML
<?xml version="1.0" encoding="UTF-8"?>

<configuration scan="true" scanPeriod="60 seconds">
  <appender name="A1" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>\${storm.home}/logs/\${logfile.name}</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>\${storm.home}/logs/\${logfile.name}.%i</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>9</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%d{yyyy-MM-dd HH:mm:ss} %c{1} [%p] %m%n</pattern>
    </encoder>
  </appender> 

  <appender name="SOCKET" class="ch.qos.logback.classic.net.SocketAppender">
    <remoteHost>${logger_host}</remoteHost>
    <port>${logger_port}</port>
    <reconnectionDelay>10000</reconnectionDelay>
    <includeCallerData>${logger_includecallerdata}</includeCallerData>
  </appender>

  <appender name="ACCESS" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>\${storm.home}/logs/access.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>\${storm.home}/logs/\${logfile.name}.%i</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>9</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%d{yyyy-MM-dd HH:mm:ss} %c{1} [%p] %m%n</pattern>
    </encoder>
  </appender> 

  <root level="INFO">
   <appender-ref ref="A1"/>
   $SOCKET_APPENDER_REF_ELEM
  </root>

  <logger name="backtype.storm.security.auth.authorizer" additivity="false">
   <level value="INFO" />
   <appender-ref ref="ACCESS" />
  </logger>
</configuration>
XML
