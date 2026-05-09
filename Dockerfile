FROM maven:3.9-eclipse-temurin-17

WORKDIR /workspace
COPY . .

RUN mvn -q -DskipTests package

ENTRYPOINT ["./bin/kuaia"]
CMD ["run", "-f", "examples/local-file-to-vector.yaml"]
