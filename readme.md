- You should only update the contents of gcp-service-account.json in project files. ETL job uses this credential to authenticate the Google Cloud resources
- When gcp-service-account.json file is ready, you can use the following command in project root folder to create docker image
```sh
    docker build . -t ozgur-firefly-case-docker-image
```
- When docker image is ready, you can create the container by using the following command
​
```sh
  docker run -p 0.0.0.0:8080:8080 --name ozgur-firefly-case-docker-container ozgur-firefly-case-docker-image:latest
```