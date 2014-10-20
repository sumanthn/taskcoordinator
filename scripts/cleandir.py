import subprocess

HADOOP_HOME="/opt/hadoop104/"
hadoopCmd=HADOOP_HOME + "bin/hadoop"
subprocess.call([hadoopCmd,"dfs","-rmr","/faultyserveripdataout"])

#now copy the file
#subprocess.call([hadoopCmd,"dfs","-put","/txmdatain"])
#always exit with 0, many times dir may not be present
exit (0)
