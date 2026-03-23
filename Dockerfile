FROM amazoncorretto:21.0.7-al2023

VOLUME /tmp

# Copy the Spring Boot fat JAR
# Build with: mvn package -pl excalibase-watcher-api --also-make -DskipTests
COPY excalibase-watcher-api/target/excalibase-watcher-api-*.jar app.jar

# JVM memory optimization for containers
ENV JAVA_OPTS="-XX:InitialRAMPercentage=50.0 \
               -XX:MaxRAMPercentage=75.0 \
               -XX:MinRAMPercentage=50.0 \
               -XX:+UseG1GC \
               -XX:MaxGCPauseMillis=200 \
               -XX:MaxMetaspaceSize=256m \
               -XX:+UseStringDeduplication \
               -XX:+UseContainerSupport"

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app.jar"]
