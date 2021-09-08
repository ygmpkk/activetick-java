FROM adoptopenjdk/openjdk8-openj9:x86_64-alpine-jre8u262-b10_openj9-0.21.0

COPY build/libs /libs
COPY application-prod.properties /

ENTRYPOINT ["java", "-Xms512m", "-Xmx6g", "-jar", "/libs/agent-0.0.1-SNAPSHOT.jar", "--spring.config.name=application-prod", "--spring.config.location=file:///"]

