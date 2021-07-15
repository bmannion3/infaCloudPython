#Introduction:

This module helps running informatica cloud jobs from unix shell or windows shell. I have been using informatica cloud for years and always had 
problems integrating it to other applications for on-demand cloud workflow execution, so I have decided to write this python module for the purpose 
mentioned. 

You can execute this python script as pre-session command or command task from informatica, you can call this from a shell script or power shell, which 
opens a lot of possibilities for informatica cloud.

#Usage:
runInfaCloudTask.py -c CREDFILE -j JOBFILE [-w] WAITTIME [-i] intervalTime [-m] maxTries [-h] [-v] 

#Required Arguments:
```
  -c CREDFILE, 		--credFile CREDFILE  		( specifies credentails file name )
  -j JOBFILE, 		--jobFile JOBFILE    		( specifies job information file )

```
#Optional Arguments:
```
  -w waitTime, 		--waitTime waitTime		( how many secs to wait while checking task status recursively )( Default = 60 )
 -i intervalTime, 	--intervalTime intervalTime	( how many secs to wait between status checks )( Default = 1 )
  -m maxTries, 		--maxTries maxTries		( how many times to retry to check the status )( Default = 1 )
  -h, --help            				( show this help message and exit )
  -v, --verbose         				( allows prograss messages to be displayed )
```


*See creadentials.properties for sample credentials file

*See sample_job.properties for sample job that you need to execute

*config.ini used to update API URLs and email notification addresses
