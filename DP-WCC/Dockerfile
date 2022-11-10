FROM golang:latest

#Specify working directory inside the image
WORKDIR /app 

#Copy the source code inside the image
COPY *.go ./
COPY *.requests ./

#build everything inside the specified directory, name it main and place it in the root dir of the image
RUN go mod init dp-wcc
RUN go build -o /main