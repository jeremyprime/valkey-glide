FROM public.ecr.aws/amazoncorretto/amazoncorretto:11

WORKDIR /app

# Install Docker CLI for container manipulation
RUN yum install -y docker && yum clean all

COPY build/libs/circuit-breaker-test-1.0-SNAPSHOT.jar /app/app.jar

CMD ["java", "-jar", "app.jar"]
