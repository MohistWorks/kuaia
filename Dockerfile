FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace
COPY . .

RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /opt/kuaia
COPY --from=build /workspace/kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar /opt/kuaia/kuaia.jar
COPY examples /opt/kuaia/examples

ENTRYPOINT ["java", "-jar", "/opt/kuaia/kuaia.jar"]
CMD ["run", "-f", "examples/local-file-to-file.yaml"]
