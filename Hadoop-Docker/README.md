## Hadoop Docker image

# First build the docker image from Dockerfile

## Build the image

```
docker build  -t sequenceiq/hadoop-ubuntu:2.6.0 .
```


# Check whether the image is created or not by listing the images

```
docker images
```


## Start a container

# Run the built hadoop docker ubuntu image

```
docker run -i -t sequenceiq/hadoop-ubuntu:2.6.0 /etc/bootstrap.sh -bash
```

## Test

# Here we run the wordcount example:

```
cd $HADOOP_PREFIX
# run the mapreduce
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar wordcount input output

# check the output
bin/hdfs dfs -cat output/*
```


