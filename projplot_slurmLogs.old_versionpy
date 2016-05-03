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
import datetime
import glob
import ntpath
from datetime import datetime, timedelta
import re
import calendar
import subprocess
import random
import string
from optparse import OptionParser

version = "2012-12-14"





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






# add the hours from a slurm log file
def getHoursSlurmLog(slurmFile, plotData):

	# open the file
	sf = open(slurmFile, 'r')

	# run gnuplot and print the result to the terminal
	popen = subprocess.Popen(['grep', '-h', projId, slurmFile], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):

		# get the start and end time of the job
		match = re.search('start=(\d+) end=(\d+).+ procs=(\d+)', line)


		# skip canceled jobs
		if match.groups()[0] == match.groups()[1]:
			continue

		# add the job to plotData
		plotData = addHours(plotData, int(match.groups()[0]), int(match.groups()[1]), int(match.groups()[2]))


	return plotData








# add the epoch time to the plotData array
def addHours(plotData, start, end, procs):

	# get the values
	global currentDate
	global days
	global maxTime
	global startDate
	global endDate

	# get the epoch time for the first day in plotData
	windowStartEpoch = calendar.timegm(startDate.timetuple())


	# subtract the window start time
	start = start - windowStartEpoch
	end = end - windowStartEpoch

	# add 'a core' to each time slot in the plotData that overlaps the job
	for i in range(start, end):

		# skip the negative numbers
		if -1 < i:

			# add 'a core' to the current time slot
			plotData[i] += procs


	return plotData








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
	valid_clusters = [ name for name in os.listdir('/bubo/sw/share/slurm/') if os.path.isdir(os.path.join('/bubo/sw/share/slurm/', name)) ]
	if options.cluster not in valid_clusters:
		sys.exit("ERROR: Invalid cluster name: %s\n\nValid clusters: %s" % (options.cluster, string.join(valid_clusters, ", "))) 






def getPrioArrow(plotData, startDate, endDate):

	global projId
	global options
	global randomName
	global coreHourLimit

	plotString = ""

	coreHourLimit = 0
	# get the accounts SLURM core hour limit
	popen = subprocess.Popen(['grep', projId, "/bubo/sw/share/slurm/%s/grantfile" % options.cluster], stdout=subprocess.PIPE)
	for line in iter(popen.stdout.readline, ""):

		match = re.search('.+:(.+):.+:.+:.+:.+:.+', line)
		coreHourLimit = int(match.groups()[0]) * 3600 # convert it to core seconds


	# get the height of the plot
	maxUsage = max(plotData)


	# check where/if the core hour limit is reached, count backwards in time
	sum = 0
	for i in reversed(range(0,len(plotData))):

		# add the seconds core usage and decrease the position counter
		sum += plotData[i]

		# if the limit has been reached
		if sum > coreHourLimit:

			# construct the plotting data
			prioPlot = [0] * len(plotData)
			prioPlot[i] = maxUsage

			# make the plot command
			plotString = "set arrow from %s,0 to %s,%s nohead" % (i, i, maxUsage*1.07)





			# stop the loop
			break



	return plotString









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
parser.add_option("-c", "--cluster", action="store", type="string", dest="cluster", help="The cluster you want to plot (default: %default)", default="kalkyl")
parser.add_option("-d", "--days", action="store", type="int", dest="days", help="The number of days you want to plot (default: %default)")
parser.add_option("-s", "--start", action="store", type="string", dest="start", help="The starting date you want to plot (format: YYYY-MM-DD)")
parser.add_option("-e", "--end", action="store", type="string", dest="end", help="The ending date you want to plot (format: YYYY-MM-DD)")
(options, args) = parser.parse_args()



# get the current date
currentDate = datetime.today().replace(hour=23, minute=59, second=59, microsecond=0)


#### CHECK INPUT ####
checkInput(options)



# get terminal size
(tHeight, tWidth) = os.popen('stty size', 'r').read().split()
tHeight = int(tHeight) - 2 # compensate for the CLI taking one row in the bottom of the screen
tWidth = int(tWidth)

# where the log files are stored
account_dir = "/bubo/sw/share/slurm/%s/accounting" % options.cluster;

# max booking time in days, to make sure all jobs are accounted for
maxTime = 15;


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
	
	# set the plot titles
	plotTitle = "Core hour usage during the period %s - %s\\nProject: %s       Cluster: %s" % (startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'), projId, options.cluster)
	plotXaxis = "Date"

	# get the ticks string
	tics = getTicks(startDate, endDate, dateSpace, True)





# if the user specified only start date (an adjusted 'missing days' is a string, not an int)
elif options.start and options.days == "30":
	startDate = datetime.strptime(options.start, '%Y-%m-%d')
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
	sys.exit("ERROR: What did you do, you broke it! This should not be able to happen.\nPlease email the command you tried to run to martin.dahlo@scilifelab.uu.se\n\n%s" % usage)









#### PAST JOBS ####

# add the maxTime padding the the start date, to catch all jobs that could have been started befor the start date and overlap in the interval of interest
startDate = startDate - timedelta(days=(maxTime))


# initiate the plotData array
plotData = [0] * (int(endDate.strftime('%s')) - int(startDate.strftime('%s'))) # one slot for each second in the windows

# initiate counter
i = 0
# for each accounting file in the slurm directory
for slurmFile in glob.glob("%s/????-??-??" % account_dir):

	# conver the file name to a date
	slurmFileDate = datetime.strptime(ntpath.basename(slurmFile), '%Y-%m-%d')

	# if the slurm log is from the correct interval
	if((slurmFileDate >= startDate) and (slurmFileDate <= endDate)):

		# parse the file and get the hours
		plotData = getHoursSlurmLog(slurmFile, plotData)









#### CURRENTLY RUNNING JOBS ####

# get all running or completing jobs in the queue
# "R,CG,CA,CF,F,NF,PR"
popen = subprocess.Popen(['jobinfo', "-A", projId, "-t", "R,CG"], stdout=subprocess.PIPE)
for line in iter(popen.stdout.readline, ""):

	# if it is a job line, get the start time and number of cores
	match = re.search('(\d+-\d+-\d+T\d+:\d+:\d+)\s+\S+\s+\S+\s+(\S+)', line)
	if match:
		
		
		# save the start time
		start = datetime.strptime(match.groups()[0], "%Y-%m-%dT%H:%M:%S").strftime('%s')
		end = currentDate.strftime('%s')
		procs = match.groups()[1]

		# add the running job to plotData
		plotData = addHours(plotData, int(start), int(end), int(procs))











#### PLOT THE DATA ####


# generate a random name for the data file
randomName = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(50))
df = open("/tmp/projplot.%s" % randomName, 'w')

# print the starting value to get the whole graph
print >>df, "0\t0"

# print the file reversed, skipping the first elements that are part of the maxTime area
for i, val in enumerate(plotData[(maxTime*86400):]):

	# only if the core usage is above 0
	if val > 0:
		print >>df, "%s\t%s" % (i, val)


# print the ending value to get the whole graph
print >>df, "%s\t0" % i 
df.close()



# send the cropped plotData to get the prioMark
# only if the current date is the end point. Not for custom ranges
coreHourLimit = 0
prioArrow = ""
if endDate == currentDate:
	prioArrow = getPrioArrow(plotData[(maxTime*86400):], startDate, endDate)


# if the prio limit has been reached, warn about it in the title
prioWarn = ""
if prioArrow != "":

	# formatting for the core hours
	import locale
	locale.setlocale(locale.LC_ALL, "sv_SE.UTF-8")

	prioWarn = "\\nNOTE: Core hour lmit (%s) reached in interval. The vertical '>' bar indicates where." % locale.format("%d", (coreHourLimit/3600), True)

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
unset key

# plot the data
plot \"/tmp/projplot.%s\" with impulse
""" % (tWidth, tHeight, plotTitle, prioWarn, plotXaxis, tics, prioArrow, randomName)
df.close()





# run gnuplot and print the result to the terminal
popen = subprocess.Popen(['gnuplot', "/tmp/projplot.%s.gnu" % randomName], stdout=subprocess.PIPE)
for line in iter(popen.stdout.readline, ""):
    print line,


# clean up
os.remove("/tmp/projplot.%s" % randomName)
os.remove("/tmp/projplot.%s.gnu" % randomName)