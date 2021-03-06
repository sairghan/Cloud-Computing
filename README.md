# Cloud-Computing

As a part of the Cloud Computing (INFSCI 2750) course, we worked on a range of bi-weekly projects involving different topics including Hadoop & MapReduce, Apache Spark, and many more.

- Mini project 1: Developed several MapReduce and Hadoop programs to analyze real anonymous logs to answer several questions based on the log and produce the ngram frequencies of the text in any given input file. Built a portable testbed using Docker (Hadoop runs in the local environment) to program and debug. Tested the performance of our algorithm using huge megabytes data generated by RandomTextWriter in Hadoop. Explored several correction methods for the programs: Unit test (verify our algorithm in the program), Job Tracker (check the status of the job of our running algorithms), Logs (use log to identify the problems). 

- Mini project 2: Developed Spark programs to perform data analytics on the ‘hetrec2011-lastfm-2k'​ dataset. This dataset contains social networking, tagging, and music artist listening information from a set of 2K users from Last.fm online music system. We have also developed programs using Spark to perform real-time log analysis and provide execution time of data processing with and without cached RDD (resilient distributed dataset). 

Configured Spark distribution on top of the Hadoop cluster. We used YARN for scheduling/running Spark applications on our setup. The entire Spark setup is configured on top of a two node Hadoop cluster.
