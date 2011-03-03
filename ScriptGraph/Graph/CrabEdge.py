#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import ScriptGraph.Helpers.LateBind as LateBind
import os.path
import re
import subprocess
import os
import sys
import commands
import glob
class CrabEdge( Edge.Edge ):
	jobscreated = 0
	jobsrunning = 0
	totaljobs = 0 
	donejobs = 0
	successfuljobs = 0
	unsuccessfuljobs = 0
	jobsretrieved = 0
	jobssubmitted = 0

	def __init__( self, crabConfig = None, cmsswConfig = None, crabReplacements = None, cmsswReplacements = None, name="" ):
		if crabConfig == None:
			crabConfig = "crab.cfg"
		if cmsswConfig == None:
			cmsswConfig = "cmssw_cfg.py"
		if crabReplacements == None:
			crabReplacements = []
		if cmsswReplacements == None:
			cmsswReplacements = []

		self.crabConfig  = os.path.abspath( crabConfig )
		self.cmsswConfig = os.path.abspath( cmsswConfig )
		self.crabReplacements = crabReplacements
		self.cmsswReplacements = cmsswReplacements
		Edge.Edge.__init__( self, name )
	
	def getStatus( self ):
		if self.statusCache == self.runningStatus:
			return "RUNNING %s/%s jobs complete" % (self.jobsretrieved - self.unsuccessfuljobs, self.totaljobs)
		else:
			return self.statusCache

	def modifyCrabConfig( self ):
		file = open( self.crabConfig, 'r' )
		input = file.read()
		file.close()
		output = self.doReplacements( input, self.crabReplacements )
		# replace the CMSSW config name too, while we're at it
		# pset = cmssw_cfg.py
		output = re.sub("\n" + r'\s*pset\s*=\s*[^\s]+\s*'+"\n" , '\npset = CMSSW-CONFIG-SCRIPTGRAPH.py\n', output)
		file = open( 'CRAB-CONFIG-SCRIPTGRAPH.cfg', 'w' )
		print "CRAB CONFIG IS\n%s\nDONE CRAB CONFIG" % output
		file.write( output )
		file.close()

	def modifyCMSSWConfig( self ):
		file = open( self.cmsswConfig, 'r' )
		input = file.read()
		file.close()
		output = self.doReplacements( input, self.cmsswReplacements )
		file = open( 'CMSSW-CONFIG-SCRIPTGRAPH.py', 'w' )
		print "CMSSW CONFIG IS\n%s\nDONE CMSSW CONFIG" % output
		file.write( output )
		file.close()
	
	def doReplacements( self, input, replacements ):
		print "Replacing with %s" % replacements
		for oneRepl in replacements:
			print "DO REPLACEMENT OF %s with %s" % (oneRepl[0], oneRepl[1] )
			output ,count = re.subn( oneRepl[0], self.lateBind( oneRepl[1] ), input )
			print "COUNT WAS %s" % count
			input = output
		return input
	
	def lateBind( self, bindobj ):
		if (isinstance( bindobj, LateBind.LateBind ) ):
			return bindobj.bind( self )
		else:
			return bindobj
	
	def setOutputRootFiles( self ):
		if self.crabWorkDir and os.path.exists( os.path.join( self.crabWorkDir, "res" ) ):
			self.clearOutputFiles()
			for file in ( os.listdir( os.path.join( self.crabWorkDir, "res" ) ) ):
				if file.endswith(".root"):
					self.addOutputFile( os.path.join( self.crabWorkDir, "res", file ) )
		

	def executeImpl( self, fileList, fileMapList ):
		# get configs
		self.modifyCrabConfig()
		self.modifyCMSSWConfig()
		# does crab create/submit
		print "executing crabs dir is %s" % os.getcwd()
		crabCreateCommand = ["crab", "-create","-cfg", os.path.join(self.workDir,"CRAB-CONFIG-SCRIPTGRAPH.cfg") ]
		print "command is %s" % crabCreateCommand
		createProcess = subprocess.Popen( args=crabCreateCommand )
		createProcess.communicate()
		if ( createProcess.wait() ):
			raise RuntimeError, "Crab appears to have failed creation. bailing"
		while True:
			submitProcess = subprocess.Popen( args=["crab", "-submit", "500"], stderr = subprocess.STDOUT,
											stdout = subprocess.PIPE )
			for line in submitProcess.stdout.readlines():
				sys.stdout.write( line )
				#sys.stdout.write( line )
				crabWorkDir = re.search(r'working directory\s+(crab_0_\d+_\d+)', line)
				if crabWorkDir:
					self.crabWorkDir = crabWorkDir.group(1)

				if line.find( "crab:  No jobs to be submitted: first create them" ) != -1:
					submitProcess.stdout.close()
					return

			print "Exit code is %s" % repr( submitProcess.wait() )

			if submitProcess.wait():
				raise RuntimeError, "There appears to be an error in crab submission. bailing"

	def checkStatusImpl( self ):
		oldcwd = os.getcwd()
		os.chdir( self.workDir )
		crabWorkDirs = glob.glob( os.path.join( os.getcwd(), "crab_0*" ) )
		crabWorkDirs.sort()
		if crabWorkDirs:
			self.crabWorkDir = crabWorkDirs[-1]
		else:
			self.setIncomplete()
			os.chdir( oldcwd )
			return
#		self.setOutputRootFiles()
#		os.chdir( self.workDir )
#		return
		if os.path.exists('COMPLETE'):
			print "Crab job '%s' cached as complete, not checking" % self.getName()
			self.setComplete()
			self.totaljobs     = 1
			self.donejobs      = 1
			self.retrievedjobs = 1
			self.setOutputRootFiles()
			os.chdir( oldcwd )
			return

		output = commands.getstatusoutput("crab -getoutput")
		print "getoutput %s" % output[1]
		cmd = "crab -status"
		output = commands.getstatusoutput(cmd)
		print "gestatus %s" % output[1]
		lines = output[1].split('\n')

		self.totaljobs = 0
		self.donejobs = 0
		self.retrievedjobs = 0
		crabWorkDir = ""
		for line in lines:
			if output[0]:
				print line
			crabWorkDir = re.search(r'working directory\s+(crab_0_\d+_\d+)', line)
			if crabWorkDir:
				self.crabWorkDir = crabWorkDir.group(1)

			if line.find("Jobs Created")!=-1:
				print "Warning, %s has a job in the 'Created' state, which may be broken"
				self.jobscreated = int(line.split()[1])
			if line.find("Jobs Submitted")!=-1:
				self.jobssubmitted = int(line.split()[1])
			if line.find("Jobs Running")!=-1:
				self.jobsrunning = int(line.split()[1])
			if line.find("Total Jobs")!=-1:
				self.totaljobs = int(line.split()[1])
			if line.find("Jobs Done")!=-1:
				self.donejobs = int(line.split()[1])
			if line.find("Jobs with Wrapper Exit Code : 0")!=-1:
				self.successfuljobs = int(line.split()[1])
			# elif to keep from matching the 0 exit code
			elif line.find("Jobs with Wrapper Exit Code : ")!=-1:
				print "Warning, %s has %s job(s) with a nonzero exit code" % (self.getName(), line.split()[1])
				self.unsuccessfuljobs = int(line.split()[1])
			if line.find("Jobs Retrieved")!=-1:
				self.jobsretrieved = int(line.split()[1])


		self.setOutputRootFiles()
		output = commands.getstatusoutput("crab -getoutput")
#		print output[1]
#		print "totaljobs is %s %s %s" % (repr( totaljobs ), donejobs, retrievedjobs )
		self.setIncomplete()
		if self.unsuccessfuljobs:
			self.setFailed()
			os.chdir( oldcwd )
		if self.jobssubmitted:
			self.setRunning()
		if self.jobsrunning:
			self.setRunning()
		if self.totaljobs == 0:
			self.setIncomplete()
		else:
			self.setRunning()

#		if (retrievedjobs != donejobs) and ( totaljobs == donejobs ):
#			self.setFail()
#			os.chdir( oldcwd )
	#		return
		if (self.totaljobs == self.successfuljobs):
			self.setComplete()
			handle=open('COMPLETE','w')
			handle.write("complete")
			handle.close()
		os.chdir( oldcwd )
#		self.setRunning()
		
