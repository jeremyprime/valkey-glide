FROM public.ecr.aws/amazoncorretto/amazoncorretto:11

WORKDIR /app

# Install tar (for Docker CLI download) and iptables (for connection error simulation)
RUN yum install -y tar gzip iptables && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz | tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    yum clean all

COPY build/libs/circuit-breaker-test-1.0-SNAPSHOT.jar /app/app.jar

CMD ["java", "-jar", "app.jar"]
