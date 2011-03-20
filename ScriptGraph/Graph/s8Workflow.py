#!/usr/bin/env python
import os, os.path, re
import ScriptGraph.Graph.Graph as Graph
import ScriptGraph.Graph.Node as NodeModule
from ScriptGraph.Graph.Node import Node as Node
import ScriptGraph.Graph.Edge as Edge
import ScriptGraph.Graph.LocalScriptEdge as LocalScriptEdge
from ScriptGraph.Graph.NullEdge import NullEdge
import ScriptGraph.Graph.CrabEdge as CrabEdge
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
from ScriptGraph.Helpers.LateBind import LateBind
from ScriptGraph.Helpers.GenTree import genTree
from ScriptGraph.Helpers.BindCrabWorkDir import BindCrabWorkDir
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Helpers.BindFileList import BindFileList
from ScriptGraph.Helpers.Miter import Miter
import ScriptGraph.Graph.CondorScriptEdge as CondorScriptEdge
g = Graph.Graph()
g.setWorkDir("/uscms_data/d2/meloam/s8workflow")

# <dataset name> <shorthand>
data_datasets = [ ['/BTau/Run2010B-Dec22ReReco_v1/AOD', 'RUN2010B'],
                  ['/BTau/Run2010A-Dec22ReReco_v1/AOD', 'RUN2010A']
]
qcd_datasets  = [ 
    ['/QCD_Pt-15to20_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd15to20' ], 
    ['/QCD_Pt-20to30_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd20to30' ], 
    ['/QCD_Pt-30to50_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd30to50' ], 
    ['/QCD_Pt-50to80_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd50to80' ], 
    ['/QCD_Pt-80to120_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd80to120' ], 
    ['/QCD_Pt-120to150_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd120to150' ], 
    ['/QCD_Pt-150_MuPt5Enriched_TuneZ2_7TeV-pythia6/Winter10-E7TeV_ProbDist_2010Data_BX156_START39_V8-v1/AODSIM', 'qcd150' ]
]


default_trigger = "HLT_BTagMu*"
# TODO: Update with the real list
# <trigger name> <shorthand name< <min run> <max run>
trigger_list_linked = { 'RUN2010B' : [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                                     [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U"   ],
                                     [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U"  ] ],
                        'RUN2010A' : [ [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ] ]
                                  }

#trigger_list_linked = { 'RUN2010B' : trigger_list_linked['RUN2010B'] }
trigger_list = [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" ],
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ],
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" ] 
]

trigger_to_sample = { 'hltdijet20u' : 'RUN2010B',
    'hltdijet30u' : 'RUN2010B',
    'hltjet20u' : 'RUN2010B',
    'hltjet10u' : 'RUN2010A'}

triggers_to_simulate = { 'hltjet10u':1, 'hltjet20u' : 1,
                         'hltdijet20u' : 0, 'hltdijet30u' : 0 }

binToAssociatedTriggers = {
                    "40to60" : [ "HLT_BTagMu_Jet10U", 
                                 "HLT_BTagMu_Jet20U" ],
                    "60to80" : [ "HLT_BTagMu_Jet10U", 
                                 "HLT_BTagMu_Jet20U",
                                 "HLT_BTagMu_DiJet20U" ],
                    "80to140" : [ "HLT_BTagMu_Jet10U", 
                                 "HLT_BTagMu_Jet20U",
                                 "HLT_BTagMu_DiJet20U" ],
                    "140to"   : [ "HLT_BTagMu_Jet10U", 
                                 "HLT_BTagMu_Jet20U",
                                 "HLT_BTagMu_DiJet20U",
                                 "HLT_BTagMu_DiJet30U"]
}

binToReweightTrigers = {
                    "60to80"  : [ "HLT_BTagMu_DiJet20U" ],
                    "80to140" : [ "HLT_BTagMu_DiJet20U" ]
}

operating_points = [ "TCHEM" ]
jet_bins = [ ["40to60", "40..60"],
             ["60to80", "60..80" ],
             ["80to140", "80..140"],
             ["140to", "140.."]
           ]

#
# Helpers
#

def run_monitor_input_helper(   g,
                                step_postfix,   
                                input_files = None, #can be a node from event generation
                                muon_pt = None,
                                jet_pt  = None,
                                trigger_name = None,
                                log = None,
                                output = None,
                                tag = None,
                                skip_events = False,
                                event_count = False,
                                additional_dependencies = False,
                                data=False,
                                reweight_trigger = False,
                                simulate_trigger = False):
    collectNode = Node( name = "collect-s8_monitor_input" + step_postfix )
    currNode    = Node( name = "s8_monitor_input" + step_postfix )
    g.addNode( collectNode )
    g.addNode( currNode )

    if input_files and isinstance( input_files, NodeModule.Node ):
        g.addEdge( input_files, collectNode, NullEdge() )
    elif input_files and isinstance( input_files, type([]) ):
        for node in input_files:
            g.addEdge( node, collectNode, NullEdge() )

    if not additional_dependencies:
        additional_dependencies = []
    for dep in additional_dependencies:
        g.addEdge( dep, collectNode, NullEdge() )
    
    commandLine = ["s8_monitor_input"]
    if log:
        commandLine.extend(["-d", log])
    if muon_pt:
        commandLine.extend(["--muon-pt=%s"% muon_pt])
    if trigger_name:
        commandLine.extend(["--trigger=%s"% trigger_name])
    if output:
        commandLine.extend(["-o", output])
    else:
        output = "output.root"
    if tag:
        commandLine.extend(["--tag", tag])
    if event_count:
        commandLine.extend(["-e", event_count])
    if skip_events:
        commandLine.extend(["-s", skip_events])
    if jet_pt:
        commandLine.extend(["--jet-pt", jet_pt])
    if data:
        commandLine.extend(["--data", data])
    if input_files and \
            (isinstance( input_files, NodeModule.Node ) or\
             isinstance( input_files, type([]) ) ):
        commandLine.extend(["-i", BindFileList( name="input.txt", filePattern="s8_tree" )])
    elif input_files:
        raise RuntimeError, "no input files? %s" % input_files

    if reweight_trigger:
        commandLine.extend(["--reweight-trigger", reweight_trigger])
    if simulate_trigger:
        commandLine.extend(["--simulate-trigger" ])
    currEdge = CondorScriptEdge.CondorScriptEdge( \
            name = "run_s8_monitor_input" + step_postfix,
            command = commandLine,
            preludeLines = [ "OLDCWD=`pwd`", "cd /uscms/home/meloam/","source sets8.sh","cd $OLDCWD" ],
            output = output,
            noEmptyFiles = True)
    g.addEdge( collectNode, currNode, currEdge )
    return currNode
    

#
# Helpers for s8_monitor_input
#
class ComputeEventCount( LateBind ):

    # Compute the number events for this particular trigger for this dataset where:

    # Events = Nevents * ( lumi_for_this_trigger ) / ( lumi_for_all_trigger )

    # eventCountNode     - the node that has the number of events in this sample
    # lumiCountNode      - the node that has the luminosity for this trigger
    # lumiCountNodeList  - the list of all the luminosities for the triggers
    #                          in this dataset
    def __init__( self, eventCountNode, trigger, lumiMiter ):
        self.eventNode     = eventNode
        self.targetTrigger = trigger
        self.lumiMiter     = lumiMiter

    def bind( self, edge ):
        events = float(self.eventNode.getValueFromOnlyOutputFile())
        lumiCount = 0
        lumisForTrigger = 0
        for onenode in self.lumiMiter.iterMany( 'trigger' ):
            lumiCount += float(onenode[0].getValueFromOnlyOutputFile())
            
            if onenode[1]['trigger'] == self.targetTrigger:
                lumisForTrigger += float( onenode[0].getValueFromOnlyOutputFile() )

        return int( ( events * lumisForTrigger ) / lumiCount)

class ComputeSkipCount( LateBind ):

    # Each dataset will correspond to many triggers, so we split it into blocks of
    # ComputeEventCount()-sized events. We want to not overlap, so we get something
    # that looks like:
    # <---TRIGGER 1---><---TRIGGER 2---><-TRIGGER 3-><-----------TRIGGER 4----------->

    # eventCountNode     - the node that has the number of events in this sample
    # trigger            - the name of the trigger, the key for lumiByTriggerList
    # lumiMiter          - the miterator of all lumi calculations

    def __init__( self, eventCountNode, trigger, lumiMiter ):
        self.eventNode = eventCountNode
        self.lumiMiter = lumiMiter
        self.targetTrigger = trigger

    def bind( self, edge ):
        events = float(self.eventNode.getValueFromOnlyOutputFile())
        lumiCount = 0
        for onenode in self.lumiMiter.iterManyValues( 'trigger' ):
            lumiCount += float(onenode.getValueFromOnlyOutputFile())
        
        currentEvent    = 0

        triggerFound = False
        for onelumi in self.lumiMiter.iterMany( 'trigger' ):
            
            # did we find the trigger
            if onelumi[1]['trigger'] == self.targetTrigger:
                triggerFound = True
            
            # is this trigger name (lexicographically) before the target
            # trigger name?
            if onelumi[1]['trigger'] < self.targetTrigger:
                currentEvent += int( events * float( onelumi[0].getValueFromOnlyOutputFile() )/ lumiCount )

        if not triggerFound:
            raise RuntimeException, "Unknown trigger"

        return currentEvent
    

# A blank node which will always allow the children to run
nullInput = Node( name = "nullInput" )
g.addNode( nullInput ) 

#
# Generate file lists (may not be too necessary, but it's pretty much free)
#
datasetSum = []
datasetSum.extend( qcd_datasets )
datasetSum.extend( data_datasets) 
for dataset in datasetSum:
    fileList = Node( name = "filelist-%s" % dataset[1] )
    datasetSearch = LocalScriptEdge.LocalScriptEdge(
                        name = "dbsQuery-%s" % dataset[1], 
                        command = "dbs search --query='find file where dataset=%s' | tail -n +5 > input.txt" % dataset[0],
                        output="input.txt", 
                        noEmptyFiles=True)
    g.addNode( fileList )
    g.addEdge( nullInput, fileList, datasetSearch )




#
# Generate trees from QCD and a child node with event counts
#
#qcdTreeNodes = {}
#eventCountNodes = {}

qcdTreeMiter    = Miter()
eventCountMiter = Miter()

for dataset in qcd_datasets:
    treeNode,produceTree = genTree( g=g,input = nullInput,
                                nodeName = "tree-%s" % dataset[1],
                                edgeName = "generateTree-%s" % dataset[1],
                                crabCfg  = "/uscms_data/d2/meloam/s8workflow/crab_mc.cfg", 
                                cmsswCfg = "/uscms_data/d2/meloam/s8workflow/cmssw_mc.py",
                                dataset  = dataset[0] )

    #qcdTreeNodes[ dataset[1] ] = treeNode
    qcdTreeMiter.add( treeNode, dataset = dataset[1] )

    #
    # we've generated the trees, add a step to extract the luminosities
    #
    eventNode = Node( name = "eventCounts-%s" % (dataset[1]) )
    # will return the crab workdir
    crabWorkDir  = BindCrabWorkDir( produceTree )
    # will return the following command line substituted in with the late-bound values
    commandLine  = BindSubstitutes( 
        "crab -report -continue %s | grep 'Total Events read: ' | awk '{print $4;}' > events.txt"
                                        ,[ crabWorkDir ] )
    getEvents = LocalScriptEdge.LocalScriptEdge( name = "getEvent-%s" % dataset[1],
        command = commandLine,
        output = "events.txt",
        noEmptyFiles=True)
    g.addNode( eventNode )
    g.addEdge( treeNode, eventNode, getEvents )
    #eventCountNodes[ dataset[1] ] = eventNode
    eventCountMiter.add( eventNode, dataset = dataset[1] )
    
#
# Generate trees from data and a child node with the luminosities
#

# The tasks to generate the trees from data
#dataTreeNodes  = []
#dataTreeByName = {}
dataTreeMiter  = Miter()
#luminosityNodes          = {} # stores luminosities
luminosityMiter     = Miter()
#luminosityByTrigger      = {}
#luminosityByTriggerMiter = Miter()
for dataset in data_datasets:

        #
        # Produce tree over the whole dataset (sans trigger)
        #
        treeNode,_ = genTree( g = g, input = nullInput,
                                nodeName = "tree-%s" % dataset[1],
                                edgeName = "generateTree-%s-%s" % (dataset[1], "default_trigger"),
                                crabCfg  = "/uscms_data/d2/meloam/s8workflow/crab_data.cfg", 
                                cmsswCfg = "/uscms_data/d2/meloam/s8workflow/cmssw_data.py",
                                dataset  = dataset[0],  
                                cmsswReplacements = [ ["CURRENTTRIGGER", default_trigger ] ],
                                crabReplacements  = [ [ "RUNSELECTION", "" ] ] )

        #
        # Now we need to run over each trigger a dataset's supposed to have
        # 
        for trigger in trigger_list_linked[dataset[1]]:
            #
            # Produce trees over a specific dataset with a specific trigger
            #
            treeNode,produceTree = genTree( g = g, input = nullInput,
                                nodeName = "tree-%s-%s" % (dataset[1], trigger[1]),
                                edgeName = "generateTree-%s-%s" % ( dataset[1], trigger[1] ),
                                crabCfg  = "/uscms_data/d2/meloam/s8workflow/crab_data.cfg", 
                                cmsswCfg = "/uscms_data/d2/meloam/s8workflow/cmssw_data.py",
                                dataset  = dataset[0],  
                                cmsswReplacements = [ ["CURRENTTRIGGER", trigger[0] ] ],
                                crabReplacements  = [ [ "RUNSELECTION", "runselection=%s-%s" %
                                                                    (trigger[2], trigger[3]) ] ] )

            #dataTreeNodes.append( treeNode )
            #dataTreeByName[ dataset[1] ] = treeNode
            dataTreeMiter.add( treeNode, dataset = dataset[1] )
            #
            # Calculate the luminosity of this (dataset*trigger)
            #

            lumiNode = Node( name = "lumi-%s-%s" % (dataset[1], trigger[1]) )
            g.addNode( lumiNode )
            
            # This is the first time latebinds have shown up in this script
            # When we build the tree, we don't know values that only get defined
            #  once the tree is run. For instance, the crab work dir gets created
            #  only when you run crab -create. Because of this, we have latebinds
            #  that get replaced at the last minute with the values you want

            # will return the crab workdir
            crabWorkDir  = BindCrabWorkDir( produceTree )
            # will return the crab workdir with the additional parts tacked on
            # the end
            lumiCalcFile = BindCrabWorkDir( produceTree, suffix = "res/lumiSummary.json" )
            # will return the following command line substituted in with the late-bound values
            commandLine  = BindSubstitutes( 
                            "crab -report -continue %s ; lumiCalc.py -i %s recorded | grep -A 2 'Recorded' | tail -n 1 | awk '{ print $4; }' > luminosity.txt"
                                                ,[ crabWorkDir, lumiCalcFile ] )

            # The actual edge
            getLuminosity = LocalScriptEdge.LocalScriptEdge( name = "getLumi-%s-%s" % ( dataset[1], trigger[1] ),
                command = commandLine,
                output = "luminosity.txt",
                noEmptyFiles=True)

            g.addEdge( treeNode, lumiNode, getLuminosity )
            
            # Add accounting
            luminosityMiter.add( lumiNode, dataset = dataset[1], trigger = trigger[1] )
#            luminosityNodes[ "%s-%s" % (dataset[1], trigger[1]) ] = lumiNode
#            if not trigger[1] in luminosityByTrigger:
#                luminosityByTrigger[ trigger[1] ] = []
#            luminosityByTrigger[ trigger[1] ].append( lumiNode )

#
# We have a bunch of individual lumis, make a node
# that is dependent on the individual ones and one w/the sum
#
luminositySumNode  = Node( name = "luminosity-sum"  )
luminosityListNode = Node( name = "luminosity-list" )
g.addNode( luminositySumNode  )
g.addNode( luminosityListNode )

for lumiNode in luminosityMiter.getValues():
    g.addEdge( lumiNode, luminosityListNode, NullEdge() )

sumLuminosity = LocalScriptEdge.LocalScriptEdge( name = "sum-all-lumis",
                                                 command = 
                            "/uscms_data/d2/meloam/s8workflow/lumiSum.py luminosity.txt ",
                                                 output  = "luminosity.txt",
                                                 addFileNamesToCommandLine = True,
                                                 noEmptyFiles = True)

g.addEdge( luminosityListNode, luminositySumNode, sumLuminosity )


#
# Now, compute the lumi per-trigger. If there's just one dataset that contains
# the trigger we're looking for, we can just pass through from the previous
# luminosity node. Otherwise, we need to add them together (usually don't add
# them)
#
#luminositySumByTrigger = {}
luminositySumMiter = Miter()
#for trigger in luminosityByTrigger:
for trigger in luminosityMiter.iterGrouped( 'trigger' ):
    # should make a wrapper for this magic
    triggerName = trigger.vals[0][1][ 'trigger' ]
    
    lumiSum = Node( "lumisum-%s" % triggerName )
    g.addNode( lumiSum )
    luminositySumMiter.add( lumiSum, trigger = triggerName ) #ByTrigger[ trigger ] = lumiSum
    
    if   len( trigger ) == 1:
        g.addEdge( trigger.getOneValue(), lumiSum, NullEdge() )
    elif len( trigger ) > 1:
        lumiCollect = Node( "lumicollect-%s" % triggerName )
        g.addNode( lumiCollect )
        for dataset in trigger:
            g.addEdge( dataset.getOneValue(), lumiCollect, NullEdge() )

        sumEdge = LocalScriptEdge.LocalScriptEdge( name = "sum-lumi-%s" % triggerName,
                                                 command =
                            "/uscms_data/d2/meloam/s8workflow/lumiSum.py luminosity.txt ",
                                                 output  = "luminosity.txt",
                                                 addFileNamesToCommandLine = True,
                                                 noEmptyFiles = True)

        g.addEdge( lumiCollect, lumiSum, sumEdge )
    else:
        raise RuntimeError, "No lumi node was found for trigger %s " % trigger.vals


#
# Run s8_monitor_input over QCD without skipping events
#

skiplessS8Monitor = {}
for opoint in operating_points:
    for bin in jet_bins:
        for sample in qcd_datasets:
            for trigger in trigger_list:
                step_postfix = "-%s-%s-%s-%s-noskip" % (sample[1],trigger[1],bin[0],opoint)
                if not opoint in skiplessS8Monitor:
                    skiplessS8Monitor[ opoint ] = {}
                if not bin[0] in skiplessS8Monitor[ opoint ]:
                    skiplessS8Monitor[ opoint ][ bin[0] ] = {}
                if not trigger[1] in skiplessS8Monitor[ opoint ][ bin[0] ]:
                    skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ] = []

                currNode = run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            trigger_name = trigger[4],
                            step_postfix = step_postfix,
                            muon_pt = "6..",
                            input_files = qcdTreeMiter.getOneValue( dataset =  sample[1] ),
                            simulate_trigger = triggers_to_simulate[ trigger[1] ])
                skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ].append( currNode )
#
# Run s8_monitor_input over data without skipping events
#
skiplessS8MonitorData = {}
skiplessS8MonitorForMerge = {}
for opoint in operating_points:
    for bin in jet_bins:
        for sample in trigger_list_linked.keys():
            for trigger in trigger_list_linked[ sample ]:
                step_postfix = "-%s-%s-%s-%s-noskip" % (sample,trigger[1],bin[0],opoint)
                merge_key    = "%s-%s" % (bin[0], opoint)

                if not opoint in skiplessS8MonitorData:
                    skiplessS8MonitorData[ opoint ] = {}
                if not bin[0] in skiplessS8MonitorData[ opoint ]:
                    skiplessS8MonitorData[ opoint ][ bin[0] ] = {}
                if not trigger[1] in skiplessS8MonitorData[ opoint ][ bin[0] ]:
                    skiplessS8MonitorData[ opoint ][ bin[0] ][ trigger[1] ] = {}
                monitor_node =\
                        run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            trigger_name = trigger[4],
                            step_postfix = step_postfix,
                            muon_pt = "6..",
                            data = "1",
                            input_files = dataTreeMiter.getValues( dataset = sample ))
                skiplessS8MonitorData[ opoint ][ bin[0] ][ trigger[1] ][ sample ] = monitor_node
                if not merge_key in skiplessS8MonitorForMerge:
                    skiplessS8MonitorForMerge[ merge_key ] = []
                skiplessS8MonitorForMerge[ merge_key ].append( monitor_node )

#
# Run s8_monitor_input over QCD, taking skipevent/eventcount into consideration
#
skippedS8Monitor = {}
for opoint in operating_points:
    for bin in jet_bins:
        for trigger in trigger_list:
            for sample in qcd_datasets:
                deps = [ # eventCountMiter.getOne( dataset = sample[1] ),
                         # we automatically include deps to input files
                         #qcdTreeNodes[ sample[1]
                         luminositySumNode,
                         luminositySumMiter.getOneValue( trigger = trigger[1] ),
                         eventCountMiter.getOneValue( dataset = sample[1] )
                       ]
                step_postfix = "-%s-%s-%s-%s" % ( sample[1], trigger[1], bin[0],opoint )
                currNode = run_monitor_input_helper( g,
                            step_postfix,
                            trigger_name = trigger[4],
                            skip_events =  ComputeSkipCount(  eventCountMiter.getOneValue( dataset = sample[1] ),
                                                                 trigger[1],
                                                                 luminosityMiter ),

                            event_count =  ComputeEventCount( eventCountMiter.getOneValue( dataset = sample[1] ),
                                                                 trigger[1],
                                                                 luminosityMiter ),
                            jet_pt = bin[1],
                            tag = opoint,
                            input_files = qcdTreeMiter.getOneValue( dataset =  sample[1] ),
                            muon_pt = "6..",
                            simulate_trigger = triggers_to_simulate[ trigger[1] ],
                            additional_dependencies = deps)
                
                nodekey = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
                if not nodekey in skippedS8Monitor:
                    skippedS8Monitor[nodekey] = {}

                skippedS8Monitor[ nodekey ][ sample[1] ] = currNode

#
# An additional wrinkle, some bin/trigger QCD combinations need to be reweighted. Fun.
#
reweightedS8Monitor = {}
for opoint in operating_points:
    for bin in jet_bins:
        for trigger in trigger_list:
            for sample in qcd_datasets:
                step_postfix= "-%s-%s-%s-%s-reweight" % ( sample[1], trigger[1], bin[0], opoint )
                step_key    = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
                collectNode = Node( name = "collect-reweight" + step_postfix )
                weightNode  = Node( name = "weight" + step_postfix )
                reweighNode = Node( name = "reweighed" + step_postfix )

                dataNode    = skiplessS8MonitorData[ opoint ][ bin[0] ][ trigger[1] ][ trigger_to_sample[ trigger[1] ]]
                
                mcNode         = skippedS8Monitor[ step_key ][ sample[1] ]
                # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
                calcWeightEdge = LocalScriptEdge.LocalScriptEdge(
                                name = "calcweight" +  step_postfix,
                                command = ["root_trigger_weights_2d",                                                                                                       bin[1],
                                                "-3..3",
                                                "weight.root",
                                                BindPreviousOutput( dataNode ),
                                                BindPreviousOutput( mcNode   ) ],
                                output = "weight.root",
                                noEmptyFiles = True )
                g.addNode( collectNode )
                g.addNode( weightNode )
                g.addEdge( dataNode, collectNode, NullEdge() )
                g.addEdge( mcNode, collectNode, NullEdge()   )
                g.addEdge( collectNode, weightNode, calcWeightEdge )

                # now, use the weights to run s8_monitor_input again
                deps = [ weightNode,
                         # we automatically include deps to input files
                         #qcdTreeNodes[ sample[1]
                         luminositySumNode,
                         luminositySumMiter.getOneValue( trigger = trigger[1] ),
                         eventCountMiter.getOneValue( dataset = sample[1] )
                       ]
    

                reweightNode = run_monitor_input_helper( g,
                            step_postfix,
                            trigger_name = trigger[4],
                            skip_events =  ComputeSkipCount(  eventCountMiter.getOneValue( dataset = sample[1] ),
                                                                 trigger[1],
                                                                 luminosityMiter ),


                            event_count =  ComputeEventCount( eventCountMiter.getOneValue( dataset = sample[1] ),
                                                                 trigger[1],
                                                                 luminosityMiter ),
                            jet_pt = bin[1],
                            tag = opoint,
                            input_files = qcdTreeMiter.getOneValue( dataset = sample[1] ),
                            muon_pt = "6..",
                            additional_dependencies = deps,
                            simulate_trigger = triggers_to_simulate[ trigger[1] ],
                            reweight_trigger = BindPreviousOutput( calcWeightEdge ) )
                
                if not step_key in reweightedS8Monitor:
                    reweightedS8Monitor[ step_key ] = {}
                reweightedS8Monitor[ step_key ][ sample[1] ] = reweightNode
                
            


#
# root_qcd to stich the qcd files together
# then combine those with root_qcd [use the luminosity reported from Data running with Trigger DiJet20u]
#
class getLumi( LateBind ):
    def __init__(self, lumiNodeList):
        self.lumiNodes = lumiNodeList
    def bind( self, edge ):
        retval = 0
        for node in self.lumiNodes:
            retval += float( self.node.getValueFromOnlyOutputFile() )
        return retval
def makeRootQCDFilenameFromInput( name ):
    pattern = "-qcd(.+?)-"
    match   = re.search( pattern, name )
    if match:
        return "pt" + match.group(1) + ".root"
    else:
        raise RuntimeError, "Unknown pt bin"


class appendCommandLineSymlink( LateBind ):
    def __init__(self, currCommand, nodeList):
        self.currCommand = currCommand
        self.nodeList    = nodeList
    def bind( self, edge ):
        for node in self.nodeList:
            for oldFile in node.getFiles()[0]:
                newFile = makeRootQCDFilenameFromInput( oldFile )
                if os.path.exists( newFile ):
                    os.unlink( newFile )
                os.symlink( oldFile, newFile )
                self.currCommand.append( newFile )
#       print "appended commandline: %s" % self.currComand
        return self.currCommand

def merge_with_root_qcd_helper( g, name , step_postfix,
                                inputNodes, triggerName, lumiMiter ):
    mergeNode   =  Node( name = name ) 
    collectNode =  Node( name = "collect-" + name ) 
    # make the node
    g.addNode( mergeNode )
    g.addNode( collectNode )
    # Add the luminosity as a dependency
    for onenode in lumiMiter.get( trigger = triggerName ):
        g.addEdge( onenode[0], collectNode, NullEdge() )

    # Add the previous S8 runs as a dependency
    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )
    
    #
    # Generate the command line
    #
    command = appendCommandLineSymlink( 
        currCommand = 
            ["root_qcd", getLumi( lumiMiter.get( trigger = triggerName ) ), "merge.root" ],
        nodeList = inputNodes
    )

    merge_edge = LocalScriptEdge.LocalScriptEdge(
                        command = command,
                        output  = "merge.root",
                        noEmptyFiles = True,
                        name = "run_root_qcd-" + step_postfix )

    g.addEdge( collectNode, mergeNode, merge_edge )



    return mergeNode
def makeRootQCDInputList( inputNodes ):
    retval = []
    for node in inputNodes:
        retval.append( makeRootQCDFilenameFromInput( node.getName() )[0] )
    return retval

def hadd_helper( g, name, inputNodes ):
    collectNode = Node( "collect-" + name )
    haddNode    = Node( "haddNode-" + name    )
    g.addNode( haddNode    )
    g.addNode( collectNode )
    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )
    haddEdge = LocalScriptEdge.LocalScriptEdge( name = "hadd-%s" % name,
                                                 command = "hadd -f merged.root ",
                                                 output  = "merged.root",
                                                 addFileNamesToCommandLine = True,
                                                 noEmptyFiles = True)
    g.addEdge( collectNode, haddNode, haddEdge )
    return g

skiplessQCDMerge = {}
skippedQCDMerge = {}
for opoint in operating_points:
    for bin in jet_bins:
        # get the list of triggers for this bin
#trigger_list = [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
#                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" ],
#                [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ],
#                [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" ] 
#]
#
#binToAssociatedTriggers = {
#                   "60to80" : [ "HLT_BTagMu_DiJet20U", "HLT_BTagMu_DiJet30U" ]
#}


        triggerNames = binToAssociatedTriggers[ bin[0 ] ]
        triggersForThisBin = []
        for wantedTrigger in triggerNames:
            for trigger in trigger_list:
                if trigger[4] == wantedTrigger:
                    triggersForThisBin.append( trigger )
                    break
    
        for trigger in triggersForThisBin:
            # only use trigger for the luminosity
            # we're using the qcd datasets for input files
            # first merge the skipless
        
            step_postfix = "-%s-%s-%s-noskip" % ( trigger[1], bin[0], opoint )
            mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                            inputNodes  = skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ],
                            triggerName = trigger[1],
                            lumiMiter   = luminosityMiter
                        )
            mergekey = "%s-%s" %(bin[0], opoint) 
            if not mergekey in skiplessQCDMerge:
                skiplessQCDMerge[ mergekey ] = []
            skiplessQCDMerge[ mergekey ].append( mergeNode )

            # now merge the skipped
            step_postfix = "-%s-%s-%s" % ( trigger[1], bin[0], opoint )
            
            # do we want to use the reweighed nodes or the old ones?
            monitorKey = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
            if monitorKey in reweightedS8Monitor:
                inputNodes = reweightedS8Monitor[ monitorKey ].values()
            else:
                inputNodes = skippedS8Monitor[ monitorKey ].values()

            mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                            inputNodes  = inputNodes,
                            triggerName = trigger[1],
                            lumiMiter   = luminosityMiter
                        )
            if not mergekey in  skippedQCDMerge:
                skippedQCDMerge[ mergekey ] = []
            skippedQCDMerge[ mergekey ].append( mergeNode )

        # END TRIGGER LOOP, THIS IS OVER bin and opoint

        hadd_helper( g, mergekey + "skip", skippedQCDMerge[ mergekey ] )
        hadd_helper( g, mergekey + "noskip", skiplessQCDMerge[ mergekey ] )
        hadd_helper( g, mergekey + "data", skiplessS8MonitorForMerge[ mergekey ] )
        # need to hadd the skipped and skipless ones to smoosh the triggers
                
#           mergeNode = Node( name="root_qcd-" + step_postfix )
#           luminosityNode = luminositySumByTrigger[ trigger[1] ]
#           qcdSets = getFilenames( edgeList = skiplessS8Monitor[ opoint ][ bin[0] ] )
#           merge_edge = LocalScriptEdge.LocalScriptEdge(
#                           command = appendCommandLine( 
#                               currCommand = 
#                                   ["root_qcd", getLumi( luminosityNode ), "merge.root" ],
#                               nodeList = skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ] ),
#                           output  = "merge.root",
#                           noEmptyFiles = True,
#                           name = "run_root_qcd-" + step_postfix )
#           g.addNode( mergeNode )
#           # Add the luminosity as a dependency
#           g.addEdge( luminosityNode, mergeNode, NullEdge() )
#           # Add the previous S8 runs as a dependency
#           for node in skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ]:
#               g.addEdge( node, mergeNode, NullEdge() )
            

def getGraph( ):
    global g
    return g

if 0:
    g.checkStatus()

    g.dumpInfo()
    g.dumpInfo2()
    g.dumpDot()
    g.pushGraph()
