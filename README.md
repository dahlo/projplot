# projplot

### A script to collect and plot core hour usage at UPPMAX
The old_version of the scipt fetched the usage data from SLURMs log files and was quite slow. I have since then constructed a database from the SLURM logs and use that database for fetching the usage information quicker. The new_version of the script, using the database, is not as portable as the old_version because of the database usage.

Usage: projplot -A <project id> [options, user -h for more help]  
Ex.  
```projplot -A b2012999```  
or  
(plots the last 90 days)  
```projplot -A b2012999 -d 90```  
