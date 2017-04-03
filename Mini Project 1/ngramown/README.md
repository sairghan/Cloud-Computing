# compile and build the jar file in netbeans in the local system.

# Jar file is located in the dist folder.

# move the file from local system to the root folder.

#move the file from linux file system to the hdfs with the below command.

	bin/hdfs dfs -put '/root/test123' /user/root

#move the input text file into /user/root/test123  ( in hdfs)

#use the below command to run the jar file in the virtual server

bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/ngramown.jar  /user/root/test123/ /user/root/test123output
