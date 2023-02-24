# Use a base image with Java pre-installed
FROM openjdk:8-jdk-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the executable JAR file to the container
COPY target/myapp.jar /app/myapp.jar

# Expose the port on which the application will run
EXPOSE 8080

# Set the command to run when the container starts
CMD ["java", "-jar", "/app/myapp.jar"]
