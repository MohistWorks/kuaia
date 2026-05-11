FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace
COPY . .

RUN mvn -q -DskipTests package \
    && VERSION=$(sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml | head -n 1) \
    && cp "kuaia-engine/target/kuaia-engine-${VERSION}-cli.jar" /workspace/kuaia.jar

FROM eclipse-temurin:17-jre

WORKDIR /opt/kuaia
COPY --from=build /workspace/kuaia.jar /opt/kuaia/kuaia.jar
COPY examples /opt/kuaia/examples

ENTRYPOINT ["java", "-jar", "/opt/kuaia/kuaia.jar"]
CMD ["run", "-f", "examples/local-file-to-file.yaml"]
