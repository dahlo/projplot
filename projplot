#!/usr/bin/python

# A script to collect and plot core hour usage
#
# Usage: projplot <project id> [<number of days to plot>]
# Ex.
# projplot b2012999
# or
# (plots the last 90 days)
# projplot b2012999 90

import sys
import os
# import datetime
import time
import glob
import ntpath
from datetime import datetime, timedelta, time
import re
import calendar
import subprocess
from random import choice
import string
from optparse import OptionParser
from ConfigParser import SafeConfigParser
import sqlite3 as lite
import numpy as np
import getpass

# if getpass.getuser() == 'dahlo':
# 	from IPython.core.debugger import Tracer


version = "2016-05-24"


### SETTINGS ###


# connect to the database
def connectDB():

	return  lite.connect('/proj/b2013023/statistics/general/general.sqlite')












# sanity check of the input, will kill the program if input makes no sense
def checkInput(options):

	global currentDate

	# if the account is missing
	if not options.account:
		sys.exit("ERROR: Project ID (-A) not specified (use -h or --help for detailed information)")

	# if the start date is after the end date
	if options.start and options.end:

		if options.start > options.end:
			sys.exit("ERROR: Start date (-s) set after end date (-e).")




	# check the format of the dates
	if options.start:

		match = re.search('\d\d\d\d-\d\d-\d\d', options.start)
		if not match:
			sys.exit("ERROR: Invalid format of start date: %s\n\nShould be in format: YYYY-MM-DD" % options.start)

	# check the format of the dates
	if options.end:

		match = re.search('\d\d\d\d-\d\d-\d\d', options.end)
		if not match:
			sys.exit("ERROR: Invalid format of end date: %s\n\nShould be in format: YYYY-MM-DD" % options.end)




	# if the user has specified start, end and days
	if options.start and options.end and options.days:
		sys.exit("ERROR: You cannot specify start date (-s) AND end date (-e) AND number of days (-d). Please remove one of them.")




	# if the user has not specified a number of days, set the standard 30 days
	if not options.days:

		# if the user did set the option, but to 0, convert it to a string to not mess up later if-statements
		if options.days == 0:
			options.days = "0"
		else:
			options.days = "30"





	# if the end date is set to after the current date
	if options.end and options.end > currentDate.strftime('%Y-%m-%d'):
		options.end = currentDate

	# if the start date is after the current date
	if options.start and options.start > currentDate.strftime('%Y-%m-%d'):
		sys.exit("ERROR: Start date (-s) is set to after the current date (%s)" % currentDate.strftime('%Y-%m-%d'))




	# check if a valid cluster name is specified
	# valid_clusters = ["kalkyl", "tintin", "halvan"]
	valid_clusters = [ name for name in os.listdir('/sw/share/slurm/') if os.path.isdir(os.path.join('/sw/share/slurm/', name)) ]
	if options.cluster not in valid_clusters:
		sys.exit("ERROR: Invalid cluster name: %s\n\nValid clusters: %s" % (options.cluster, string.join(valid_clusters, ", "))) 









# generate the xtics string
def getTicks(startDate, endDate, space, dates=False):

	# initiate
	tics = ""

	# get the number of days to generate
	days = int((endDate - startDate).days)


	# for each day, in reverse to get the time to flow the right way
	for i in reversed(range(0,days+1)):

		# make sure the ticks doesnt come too close and makes it unreadable, mod space
		if (i % round(0.5+(days / (tWidth/space)))) == 0:

			# if it is dates that should be printed
			if dates:

				# add the date to the ticks string
				tics += "\"%s \" %s, " % ((endDate - timedelta(days=i)).strftime('%Y-%m-%d'), ((days-i)*86400))

			# if it day numbers that should be printed
			else:
				tics += "\"%s\" %s, " % (i, ((days-i)*86400))

	
	# return the string after removing the last comma and space
	return tics[:-2]












# get the finished jobs from the database
def getJobs(cur, startDate, endDate, projId):

	global options


	# get jobs
	query = "SELECT start,end,cores,job_id FROM jobs WHERE proj_id = '%s' AND start < %s AND end > %s and cluster='%s';" % (projId, (int(endDate.strftime('%s'))), (int(startDate.strftime('%s'))), options.cluster)
	cur.execute(query)



	# get the start points epoch time
	windowStartEpoch = calendar.timegm(startDate.timetuple())
	jobs = cur.fetchall()

	# initiate stuff
	starts = np.empty([0,2])
	stops = np.empty([0,2])
	jids = dict()


	# loop over the jobs
	for job in jobs:


		# count the level to start from
		if int(((job[0]) - windowStartEpoch) ) < 0:

			# Tracer()()

			# save the stop
			starts = np.append(starts, [[0 , int(job[2])]], 0) 
			stops = np.append(stops, [[int(((job[1]) - windowStartEpoch) ) , int(job[2])]] , 0) 
			jids[job[3]] = 1

			continue

		# save the info
		starts = np.append(starts, [[int(((job[0]) - windowStartEpoch) ) , int(job[2])]], 0) 
		stops = np.append(stops, [[int(((job[1]) - windowStartEpoch) ) , int(job[2])]], 0)
		jids[job[3]] = 1



	# return the arrays
	return [starts, stops, jids]

		










# add time to the plotData
def addPlotData(plotData, starts, stops):

	# check if there are any jobs
	if (np.shape(starts)[0] == 0) and (np.shape(stops)[0] == 0):
		# don't do anything if there aren't any jobs
		return plotData


	# initiate stuff
	starts_times = np.zeros( (len(starts)) ) 
	starts_procs = np.zeros( (len(starts)) )

	stops_times = np.zeros( (len(stops)) ) 
	stops_procs = np.zeros( (len(stops)) ) 

	level = 0




	# muck around with the data structures (i must be stupid)

	# sort the arrays first!
	starts = starts[starts[:,0].argsort()]
	stops = stops[stops[:,0].argsort()]


	for i,val in enumerate(starts):
		starts_times[i] = val[0]
		starts_procs[i] = val[1]


	for j,val in enumerate(stops):
		stops_times[j] = val[0]
		stops_procs[j] = val[1]



	# save the lengths
	starts_length = len(starts_times)
	stops_length = len(stops_times)
	plotData_length = len(plotData)

	# Tracer()()

	run = True
	i = 0 # reset
	j = 0 # reset
	k = 0 # reset

	while run:

		# get the next step
		try:
			start_current = starts_times[i]

		except IndexError:

			start_current = 9999999999999


		try:
			stop_current = stops_times[j]

		except IndexError:

			stop_current = 9999999999999


		# print "%s\t%s" % (start_current, stop_current)
		# check if the end of both has been reached
		if start_current >= 9999999999999 and stop_current >= 9999999999999:

			# end the loop
			break



		# if a start is closest
		if start_current < stop_current:

			# add to the interval
			plotData[k:start_current] = level

			# update the level
			level += starts_procs[i]

			# take a step in the plotData array
			k = start_current

			# take a step in the start array
			i += 1




		# if a stop is closest
		else:

			# change the level in the interval
			plotData[k:stop_current] = level

			# update the level
			level -= stops_procs[j]

			# take a step in the plotData array
			k = stop_current

			# take a step in the start array
			j += 1




	# fill the rest of the plotData with the current level
	plotData[k:] = level

	return plotData











def getRunningJobs(projId, startDate, endDate, starts, stops, jids):

	global plotData
	global currentDate
	global options


	# get all running or completing jobs in the queue
	# "R,CG,CA,CF,F,NF,PR"
	popen = subprocess.Popen(['jobinfo', "-A", projId, "-t", "R,CG", "--clusters", options.cluster], stdout=subprocess.PIPE)
	windowStartEpoch = calendar.timegm(startDate.timetuple())
	for line in iter(popen.stdout.readline, ""):

		# end if the running jobs are all processed
		match = re.search('^Nodes in use', line)
		if match:
			break


		# if it is a job line, get the start time and number of cores
		match = re.search('^ (\d+)\s.+\s(\d+-\d+-\d+T\d+:\d+:\d+)\s+\S+\s+\S+\s+(\S+)', line)
		if match:



			
			# save the start time
			start = int(datetime.strptime(match.groups()[1], "%Y-%m-%dT%H:%M:%S").strftime('%s'))
			stop = int(currentDate.strftime("%s"))
			procs = int(match.groups()[2])
			jid = int(match.groups()[0])

			# next job if it does not overlap the requested window
			if( (int(start) > int(endDate.strftime('%s')))):
				continue

			# if the start point is before the window, set it to the window start
			if (int(stop) < int(startDate.strftime('%s'))):
				start = windowStartEpoch


			# add the running job to plotData
			# subtract the window start time
			starts = np.append( starts, [[int(start - windowStartEpoch), procs]], 0 )
			stops = np.append( stops, [[int(stop - windowStartEpoch), procs]], 0 )

	
	# return the jobs
	return [starts, stops, jids]








# add the hours from a slurm log file
def getHoursSlurmLog(slurmFile, starts, stops, jids):

	global plotData
	global options

	# initiate
	windowStartEpoch = calendar.timegm(startDate.timetuple())


	# where the log files are stored
	account_dir = "/sw/share/slurm/%s/accounting" % options.cluster;


	# find all rows containing the project
	popen = subprocess.Popen(['grep', '-h', projId, "%s/%s" % (account_dir, slurmFile)], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):

		# get the start and end time of the job
		match = re.search('jobid=(\d+).+ start=(\d+) end=(\d+).+ procs=(\d+)', line)

		start =  int(match.groups()[1]) - windowStartEpoch
		stop = int(match.groups()[2]) - windowStartEpoch
		procs = int(match.groups()[3])
		jid = int(match.groups()[0])

		# skip the job if it is already added from another source
		if jid in jids:
			continue

		# if it is a canceled job, don't append it
		if start == stop:
			continue

		# if the job started before the window
		if start < 0:
			start = 0


		starts = np.append( starts, [[start, procs]], 0 )
		stops = np.append( stops, [[stop, procs]], 0 )
		jids[jid] = 1



	# return the jobs
	return [starts, stops, jids]		













# pinpoint where the project passed it's core hour limit
def getPrioArrow(plotData, startDate, endDate):

	global projId
	global options
	global randomName
	global coreHourLimit
	global ymax

	plotString = ""

	coreHourLimit = 0
	# get the accounts SLURM core hour limit
	popen = subprocess.Popen(['grep', projId, "/sw/share/slurm/%s/grantfile" % options.cluster], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):

		match = re.search('.+:(.+):.+:.+:.+:.+:.+', line)
		coreHourLimit = int(match.groups()[0]) * 3600 # convert it to core seconds

	# get the height of the plot
	ymax = np.amax(plotData)



	# check where/if the core hour limit is reached, count backwards in time
	# step X seconds at a time to speed things up
	X = 120
	sum = 0
	for i in range(len(plotData)-1, 0, -X):

		# add the seconds core usage and decrease the position counter
		sum += plotData[i]*X

		# if the limit has been reached
		if sum > coreHourLimit:

			# make the plot command
			plotString = "set arrow from %s,0 to %s,%s nohead" % (i, i, ymax*1.07)





			# stop the loop
			break


	return plotString










# calculate the core hour usage in the interval
def getCoreHourUsage(plotData):

	# prio0 = time.time()
	global options


	# accumulate the core hours
	# step X seconds at a time to speed things up
	X = 30
	sum = 0
	for i in range(0, len(plotData), X):

		# add the seconds core usage and decrease the position counter
		sum += plotData[i]*X

	return sum





# plot the data
def plot():

	global plotData
	global ymax

	# generate a random name for the data file
	randomName = ''.join(choice(string.ascii_uppercase + string.digits) for x in range(50))
	df = open("/tmp/projplot.%s" % randomName, 'w')

	# print the starting value to get the whole graph
	print >>df, "0\t0"

	# print the plotting data, but only sample it every X seconds (MASSIVE speedup)
	X = 120
	for i in range(0,len(plotData),X):

		# only if the core usage is above 0
		if plotData[i] > 0:
			print >>df, "%s\t%s" % (i, plotData[i])


	# print the ending value to get the whole graph
	print >>df, "%s\t0" % i 
	df.close()

	# send the cropped plotData to get the prioMark
	# only if the current date is the end point. Not for custom ranges
	prioArrow = ""
	if endDate == currentDate:
		prioArrow = getPrioArrow(plotData, startDate, endDate)

	# get the total number of core cours used in the interval
	ch_used = getCoreHourUsage(plotData)

	# formatting for the core hours
	import locale
	locale.setlocale(locale.LC_ALL, "sv_SE.UTF-8")

	# get the accounts SLURM core hour limit
	popen = subprocess.Popen(['grep', projId, "/sw/share/slurm/%s/grantfile" % options.cluster], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):

		match = re.search('.+:(.+):.+:.+:.+:.+:.+', line)
		coreHourLimit = int(match.groups()[0]) * 3600 # convert it to core seconds


	# if the project has no allocation on current cluster
	try:
		coreHourLimit = coreHourLimit
		
	except UnboundLocalError:
		coreHourLimit = 0.0001
		
		
	
	# if the prio limit has been reached, warn about it in the title
	prioWarn = ""
	if prioArrow != "":
		
		percentage = "{0:.2f}".format(( (ch_used/3600) / (coreHourLimit/3600))*100)

		prioWarn = "\\nNOTE: Core hour lmit (%s) reached in interval.\\nThe vertical '>' bar indicates where.\\nUsed: ~%s (%s%s)" % (locale.format("%d", (coreHourLimit/3600), True),  locale.format("%d", (ch_used/3600), True), percentage, "%")

	# else just inform how many hours used
	else:
		
		percentage = "{0:.2f}".format(( (ch_used/3600) / (coreHourLimit/3600))*100)
		
		prioWarn = "\\nCore hours used in interval: ~%s (%s%s)\\nLimit: %s" % (locale.format("%d", (ch_used/3600), True), percentage, "%", locale.format("%d", (coreHourLimit/3600), True))



	# set the y range to avoid error message if empty
	yrange = "set yrange [0:%s]" % max(1, 1.1*ymax)

	# write the gnuplot script
	df = open("/tmp/projplot.%s.gnu" % randomName, 'w')
	print >>df, """
	# plot to ascii
	set terminal dumb size %s %s
	 
	# rename the axis
	set ylabel 'Cores used'
	set title "%s%s"
	set xlabel '%s'
	set xtics(%s)
	set tic scale 0
	%s
	%s
	unset key

	# plot the data
	plot \"/tmp/projplot.%s\" with impulse
	""" % (tWidth, tHeight, plotTitle, prioWarn, plotXaxis, tics, prioArrow, yrange, randomName)
	df.close()




	# run gnuplot and print the result to the terminal
	# print 'gnuplot44', "/tmp/projplot.%s.gnu" % randomName
	popen = subprocess.Popen(['gnuplot44', "/tmp/projplot.%s.gnu" % randomName], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):
	    print line,

	# clean up
	os.remove("/tmp/projplot.%s" % randomName)
	os.remove("/tmp/projplot.%s.gnu" % randomName)




















usage = """Usage: %prog -A <proj-id> [options]

More details: http://www.uppmax.uu.se/plotting-your-core-hour-usage

Example runs:

# Plot the last 30 days of project <proj>
%prog -A <proj>

# Plot the last 30 days of project <proj> on cluster <cluster>
%prog -A <proj> -c <cluster>

# Plot the last <n> days of project <proj>
%prog -A <proj> -d <n>

# Plot the usage for project <proj> since <date>
%prog -A <proj> -s <date>

# Plot the usage for project <proj> between <date_1> and <date_2>
%prog -A <proj> -s <date_1> -e <date_2>

# Plot the usage for project <proj> between <date_1> and <date_2>, on cluster <cluster>
%prog -A <proj> -s <date_1> -e <date_2> -c <cluster>

# Plot the usage for project <proj> between date <date_1> and <days> days later
%prog -A <proj> -s <date_1> -d <days>

# Plot the usage for project <proj> between date <date_1> and <days> days earlier
%prog -A <proj> -e <date_1> -d <days>
"""


# parse the options
parser = OptionParser(usage=usage)
parser.add_option("-A", "--account", action="store", type="string", dest="account", help="Your UPPMAX project ID")
parser.add_option("-c", "--cluster", action="store", type="string", dest="cluster", help="The cluster you want to plot (default: current cluster)", default=os.environ['SNIC_RESOURCE'])
parser.add_option("-d", "--days", action="store", type="int", dest="days", help="The number of days you want to plot (default: %default)")
parser.add_option("-s", "--start", action="store", type="string", dest="start", help="The starting date you want to plot (format: YYYY-MM-DD)")
parser.add_option("-e", "--end", action="store", type="string", dest="end", help="The ending date you want to plot (format: YYYY-MM-DD)")
(options, args) = parser.parse_args()



# get the current date
currentDate = datetime.today() #.replace(hour=23, minute=59, second=59, microsecond=0)

#### CHECK INPUT ####
checkInput(options)



# get terminal size
(tHeight, tWidth) = os.popen('stty size', 'r').read().split()
tHeight = int(tHeight) - 2 # compensate for the CLI taking one row in the bottom of the screen
tWidth = int(tWidth)

# where the log files are stored
account_dir = "/sw/share/slurm/%s/accounting" % options.cluster;


# set the space between ticks in the ticks string
dateSpace = 12
daySpace = 6


# initialize variables
endDate = ""
startDate = ""
plotTitle = ""
plotXaxis = ""
daysPlural = ""
tics = ""
projId = options.account


# if the user only specified days
if options.days and not options.start and not options.end:
	endDate = currentDate
	startDate = (currentDate - timedelta(days=int(options.days))).replace(hour=0, minute=0, second=0)

	# add an s at the end if there are more than 1 day
	if options.days > 1:
		daysPlural = 's'

	# set the plot titles
	plotTitle = "Core hour usage during the last %s day%s\\nProject: %s       Cluster: %s" % (options.days, daysPlural, projId, options.cluster)
	plotXaxis = "Day%s ago" % daysPlural

	# get the ticks string
	tics = getTicks(startDate, endDate, daySpace, False)





# if the user specified both start and end date
elif options.start and options.end:
	startDate = datetime.strptime(options.start, '%Y-%m-%d')
	endDate = datetime.strptime(options.end + " 23:59:59", '%Y-%m-%d %H:%M:%S')  # set the time to midnight that day to include all jobs that day

	# check if the end date is after the current date
	if endDate > currentDate:
		endDate = currentDate
	
	# set the plot titles
	plotTitle = "Core hour usage during the period %s - %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)





# if the user specified only start date (an adjusted 'missing days' is a string, not an int)
elif options.start and options.days == "30":
	startDate = datetime.strptime(options.start, '%Y-%m-%d')
	endDate = datetime.strptime(options.start + " 23:59:59", '%Y-%m-%d %H:%M:%S') + timedelta(days=int(options.days)) # set the time to midnight that day to include all jobs that day

	# check if the end date is after the current date
	if endDate > currentDate:
		endDate = currentDate
	
	# set the plot titles
	plotTitle = "Core hour usage during the period %s - %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)





# if the user specified start date and days
elif options.start and options.days:
	startDate = datetime.strptime(options.start, '%Y-%m-%d')
	endDate = datetime.strptime(options.start + " 23:59:59", '%Y-%m-%d %H:%M:%S') + timedelta(days=int(options.days)) # set the time to midnight that day to include all jobs that day

	# check if the end date is after the current date
	if endDate > currentDate:
		endDate = currentDate
	
	# set the plot titles
	plotTitle = "Core hour usage during the period %s - %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)




# if the user specified end date and days
elif options.end and options.days:
	startDate = datetime.strptime(options.end, '%Y-%m-%d') - timedelta(days=int(options.days))
	endDate = datetime.strptime(options.end + " 23:59:59", '%Y-%m-%d %H:%M:%S')  # set the time to midnight that day to include all jobs that day

	# check if the end date is after the current date
	if endDate > currentDate:
		endDate = currentDate
	
	# set the plot titles
	plotTitle = "Core hour usage during the period %s - %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)





# if the user specified only a start date
elif options.start and not options.end:
	endDate = currentDate
	startDate = datetime.strptime(options.start, '%Y-%m-%d')

	# set the plot titles
	plotTitle = "Core hour usage since %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)




# if the user did something i could not predict
else:
	sys.exit("ERROR: What did you do, you broke it! This should not be able to happen.\nPlease email the command you tried to run to support@uppmax.uu.se\n\n%s" % usage)




# get the current date
# currentDate = datetime.today().replace(hour=22, minute=59, second=59, microsecond=0)
# startDate = (currentDate - timedelta(days=int(30))).replace(hour=0, minute=0, second=0)
# endDate = currentDate


# print "init"

#### INITIALIZE ####
# initiate the plotData array
# plotData = [0] * int((int(endDate.strftime('%s')) - int(startDate.strftime('%s'))) / resolution) # one slot for each second in the windows
plotData = np.zeros( (int((int(endDate.strftime('%s')) - int(startDate.strftime('%s'))) )) ) 

# connect to db
db = connectDB()
# create cursor
cur = db.cursor()


#### PAST JOBS ####
# get finished jobs
# print "get finished"
[starts, stops, jids] = getJobs(cur, startDate, endDate, projId)



#### YESTERDAYS'S JOBS #### 
# get jobs that has finished yesterday, since the db was updated
# print "get finished today"
# only if the time is in the interval between midnight and when the db is updated
now = datetime.now().time()
if now>= time(00,00) and now <= time(06,30):
	[starts, stops, jids] = getHoursSlurmLog((currentDate-timedelta(days=1)).strftime("%Y-%m-%d"), starts, stops, jids)


#### TODAY'S JOBS ####
# get jobs that has finished today, since the db was updated
# print "get finished today"
[starts, stops, jids] = getHoursSlurmLog(currentDate.strftime("%Y-%m-%d"), starts, stops, jids)


#### RUNNING JOBS ####
# print "get running"
# get currently running jobs
[starts, stops, jids] = getRunningJobs(projId, startDate, endDate, starts, stops, jids)

#### SUMMARIZE THE DATA ####
# summarize the jobs
# print "summarize"
ymax = 0 # to avoid calling np.amax() twice
plotData = addPlotData(plotData, starts, stops)

#### PLOT THE DATA ####
# plot the data
# print "plotting"
plot()
