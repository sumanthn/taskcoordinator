import subprocess
import time

HADOOP_HOME="/opt/hadoop104/"
hadoopCmd=HADOOP_HOME + "bin/hadoop"
localtime   = time.localtime()
timeString  = time.strftime("%Y%m%d%H%M", localtime)

destFile="/opt/stagedir/faultyserverip-" + timeString +".out" 
#print destFile
#how to copy multiple parts output
subprocess.call([hadoopCmd,"dfs","-copyToLocal","/faultyserveripdataout/part-00000",destFile])

