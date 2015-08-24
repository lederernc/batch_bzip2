#!/usr/bin/env python
import threading
import Queue
import os
import sys
import datetime as DT
from optparse import OptionParser

parser = OptionParser()
WORKERS = os.sysconf('SC_NPROCESSORS_ONLN')
today   = DT.datetime.today()

parser.add_option("-c", "--stdout",
			dest="stdout",
			action="store_true",
			default=False,
			help="Compress or decompress to standard output.")

parser.add_option("-d", "--decompress",
			dest="decompress",
			action="store_true",
			default=False,
			help="Force  decompression.")

parser.add_option("-z", "--compress",
			dest="compress",
			action="store_true",
			default=False,
			help="The complement to -d:  forces  compression,  regardless  of  the invocation name.")

parser.add_option("-t","--test",
			dest="test",
			action="store_true",
			default=False,
			help="Check integrity of the specified file(s), but don't decompress them.")

parser.add_option("-f", "--force",
			dest="force",
			action="store_true",
			default=False,
			help="Force  overwrite  of  output  files.")

parser.add_option("-k","--keep",
			dest="keep",
			action="store_true",
			default=False,
			help="Don't delete")
			
parser.add_option("-s","--small",
			dest="small",
			action="store_true",
			default=False,
			help="Reduce memory usage.")
			
parser.add_option("-q","--quiet",
			dest="quiet",
			action="store_true",
			default=False,
			help="Suppress non-essential warning messages.")
			
parser.add_option("-v","--verbose",
			dest="verbose",
			action="store_true",
			default=False,
			help="Lots of output.")

(options, args) = parser.parse_args()

cmd = "bzip2 "

if options.stdout:
	cmd += " -c "
if options.decompress:
	cmd += " -d "
if options.compress:
	cmd += " -z "
if options.test:
	cmd += " -t "
if options.force:
	cmd += " -f "
if options.keep:
	cmd += " -k "
if options.small:
	cmd += " -s "
if options.quiet:
	cmd += " -q "
if options.verbose:
	cmd += " -v "

class Worker(threading.Thread):
	
	def __init__(self,queue):
		self.__queue = queue
		threading.Thread.__init__(self)
		
	def run(self):
		while True:
			try:
				file = self.__queue.get(True,4)
				bz_cmd = cmd + file
				a = os.popen(bz_cmd)
				a.close()
				self.__queue.task_done()
				print ("bz_cmd: %s has completed" % bz_cmd)
			except Queue.Empty:
				break
				
queue = Queue.Queue(WORKERS + 1)

for i in range(WORKERS):
	Worker(queue).start()
	
try:
	for file in args:
		print ("adding %s to the queue" % file)
		queue.put(file)
		
except (KeyboardInterrupt, SystemExit):
	sys.exit()
	
else:
	queue.join()
	print ('Batch-bzip2 task completed. Total time elapsed: %d seconds' %(DT.datetime.today() - today).seconds)