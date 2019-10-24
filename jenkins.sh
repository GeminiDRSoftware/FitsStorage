docker run -p 8080:8080 --privileged --name=jenkins -v /tmp:/tmp -v /var/run/docker.sock:/var/run/docker.sock -d gemini-jenkins:latest
