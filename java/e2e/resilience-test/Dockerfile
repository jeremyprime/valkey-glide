FROM public.ecr.aws/amazoncorretto/amazoncorretto:21

WORKDIR /app

# Install Docker CLI (needed for network partition test to pause/unpause containers)
RUN amazon-linux-extras install docker -y 2>/dev/null || yum install -y docker && yum clean all

COPY build/libs/resilience-test-1.0-SNAPSHOT.jar /app/app.jar

# Default to network partition test; override with MAIN_CLASS env var
ENV MAIN_CLASS=com.company.glide.resilience.NetworkPartitionTest
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -cp app.jar $MAIN_CLASS"]
