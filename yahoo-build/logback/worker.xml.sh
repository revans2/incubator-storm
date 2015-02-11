#!/bin/bash

# Error on anything that goes wrong.
set -e

cat <<XML
<?xml version="1.0" encoding="UTF-8"?>

<configuration scan="true" scanPeriod="60 seconds">
  <appender name="A1" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>\${storm.home}/logs/\${logfile.name}</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>\${storm.home}/logs/\${logfile.name}.%i.gz</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>1</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>250MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%d{yyyy-MM-dd HH:mm:ss} %c{1} [%p] %m%n</pattern>
    </encoder>
  </appender> 

  <appender name="SYSLOG" class="ch.qos.logback.classic.net.SyslogAppender">
    <syslogHost>${syslog_host}</syslogHost>
    <facility>${syslog_facility}</facility>
    <suffixPattern>[%p] [\${storm.id}:\${worker.port}] %m%n</suffixPattern>
  </appender>

 <appender name="STDERR" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>\${storm.home}/logs/\${logfile.name}.err</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>\${storm.home}/logs/\${logfile.name}.err.%i.gz</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>1</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>250MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%m</pattern>
    </encoder>
  </appender> 

 <appender name="STDOUT" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>\${storm.home}/logs/\${logfile.name}.out</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>\${storm.home}/logs/\${logfile.name}.out.%i.gz</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>4</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%m</pattern>
    </encoder>
  </appender> 

  <root level="INFO">
   <appender-ref ref="A1"/>
   <appender-ref ref="SYSLOG"/>
  </root>

  <logger name="STDERR" additivity="false">
    <level value="INFO" />
    <appender-ref ref="STDERR" />
  </logger>

  <logger name="STDOUT" additivity="false">
    <level value="INFO" />
    <appender-ref ref="STDOUT" />
  </logger>
</configuration>
XML
