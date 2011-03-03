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
trigger_list_linked = { 'RUN2010B' : [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818" ],
                                     [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294"   ],
                                     [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116"   ] ],
                        'RUN2010A' : [ [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039" ] ]
                                  }

#trigger_list_linked = { 'RUN2010B' : trigger_list_linked['RUN2010B'] }
trigger_list = [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" ],
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ],
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" ] 
]

binToAssociatedTriggers = {
                    "60to80" : [ "HLT_BTagMu_DiJet20U", "HLT_BTagMu_DiJet30U" ]
}

binToReweightTrigers = {
                    "60to80"  : [ "HLT_BTagMu_DiJet20U" ],
                    "80to120" : [ "HLT_BTagMu_DiJet20U" ]
}

operating_points = [ "TCHEM" ]
jet_bins = [ ["60to80", "60..80" ] ]

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
                                data=False):
    collectNode = Node( name = "collect-s8_monitor_input" + step_postfix )
    currNode    = Node( name = "s8_monitor_input" + step_postfix )
    g.addNode( collectNode )
    g.addNode( currNode )

    if input_files and isinstance( input_files, NodeModule.Node ):
        g.addEdge( input_files, collectNode, NullEdge() )
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
    if input_files and isinstance( input_files, NodeModule.Node ):
        commandLine.extend(["-i", BindFileList( name="input.txt" )])

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
    
    # eventCountNode     - the node that has the number of events in this sample
    # lumiCountNode      - the node that has the luminosity for this trigger
    # lumiCountNodeList  - the list of all the luminosities for the triggers
    #                          in this dataset
    def __init__( self, eventCountNode, lumiCountNode, lumiCountNodeList ):
        self.eventNode = eventNode
        self.lumiCountNodeList = lumiCountNodeList
        self.lumiCountNode = lumiCountNode

    def bind( self, edge ):
        events = float(self.eventNode.getValueFromOnlyOutputFile())
        lumiCount = 0
        for onenode in self.lumiCountNodeList:
            lumiCount += float(onenode.getValueFromOnlyOutputFile())
        #lumisForTrigger = 0
        #for onenode in self.lumiCountNode:
        #   lumisForTrigger += float(onenode.getValueFromOnlyOutputFile())
        lumisForTrigger = float( onenode.getValueFromOnlyOutputFile() )
        print "%s %s %s" % (events, lumisForTrigger, lumiCount)
        return int( ( events * lumisForTrigger ) / lumiCount)

class ComputeSkipCount( LateBind ):
    # eventCountNode     - the node that has the number of events in this sample
    # trigger            - the name of the trigger, the key for lumiByTriggerList
    # lumiByTriggerList  - a dict of lumi nodes, keys are trigger names
    # lumiCountNodeList  - the list of all the luminosities for the triggers
    #                          in this dataset
    def __init__( self, eventCountNode, trigger, lumiByTriggerList, lumiCountNodeList ):
        self.eventNode = eventCountNode
        self.lumiCountNodeList = lumiCountNodeList
        self.lumiByTriggerList = lumiByTriggerList
        self.targetTrigger = trigger

    def bind( self, edge ):
        #lumis  = float(self.lumiCountNodeList.getValueFromOnlyOutputFile())
        events = float(self.eventNode.getValueFromOnlyOutputFile())
        lumiCount = 0
        for onenode in self.lumiCountNodeList:
            lumiCount += float(onenode.getValueFromOnlyOutputFile())
        
        currentEvent    = 0
        keylist = self.lumiByTriggerList.keys()
        for trigger in keylist:
            if trigger == self.targetTrigger:
                return currentEvent
            for onenode in self.lumiByTriggerList[ trigger ]:
                currentEvent += int( events * float( onenode.getValueFromOnlyOutputFile() )/ lumiCount )

        raise RuntimeException, "Unknown trigger"
    

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
qcdTreeNodes = {}
eventCountNodes = {}
for dataset in qcd_datasets:
    treeNode,produceTree = genTree( g=g,input = nullInput,
                                nodeName = "tree-%s" % dataset[1],
                                edgeName = "generateTree-%s" % dataset[1],
                                crabCfg  = "/uscms_data/d2/meloam/s8workflow/crab_mc.cfg", 
                                cmsswCfg = "/uscms_data/d2/meloam/s8workflow/cmssw_mc.py",
                                dataset  = dataset[0] )

    qcdTreeNodes[ dataset[1] ] = treeNode


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
    eventCountNodes[ dataset[1] ] = eventNode
    

#
# Generate trees from data and a child node with the luminosities
#

# The tasks to generate the trees from data
dataTreeNodes = []
dataTreeByName = {}
luminosityNodes = {} # stores luminosities
luminosityByTrigger = {}
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

            dataTreeNodes.append( treeNode )
            dataTreeByName[ dataset[1] ] = treeNode     
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
            luminosityNodes[ "%s-%s" % (dataset[1], trigger[1]) ] = lumiNode
            if not trigger[1] in luminosityByTrigger:
                luminosityByTrigger[ trigger[1] ] = []
            luminosityByTrigger[ trigger[1] ].append( lumiNode )

#
# We have a bunch of individual lumis, make a node
# that is dependent on the individual ones and one w/the sum
#
luminositySumNode  = Node( name = "luminosity-sum"  )
luminosityListNode = Node( name = "luminosity-list" )
g.addNode( luminositySumNode  )
g.addNode( luminosityListNode )

for lumiNode in luminosityNodes.values():
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
luminositySumByTrigger = {}
for trigger in luminosityByTrigger:
    lumiSum = Node( "lumisum-%s" % trigger )
    g.addNode( lumiSum )
    luminositySumByTrigger[ trigger ] = lumiSum
    if   len( luminosityByTrigger[ trigger ] ) == 1:
        g.addEdge( luminosityByTrigger[ trigger ][0], lumiSum, NullEdge() )
    elif len( luminosityByTrigger[ trigger ] ) > 1:
        lumiCollect = Node( "lumicollect-%s" % trigger )
        g.addNode( lumiCollect )
        for node in luminosityByTrigger[ trigger ]:
            g.addEdge( node, lumiCollect, NullEdge() )

        sumEdge = LocalScriptEdge.LocalScriptEdge( name = "sum-lumi-%s" % trigger,
                                                 command =
                            "/uscms_data/d2/meloam/s8workflow/lumiSum.py luminosity.txt ",
                                                 output  = "luminosity.txt",
                                                 addFileNamesToCommandLine = True,
                                                 noEmptyFiles = True)

        g.addEdge( lumiCollect, lumiSum, sumEdge )
    else:
        raise RuntimeError, "No lumi node was found for trigger %s " % trigger


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
                            input_files = qcdTreeNodes[ sample[1] ])
                skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ].append( currNode )
#
# Run s8_monitor_input over data without skipping events
#

skiplessS8MonitorData = {}
for opoint in operating_points:
    for bin in jet_bins:
        for sample in data_datasets:
            for trigger in trigger_list:
                step_postfix = "-%s-%s-%s-%s-noskip" % (sample[1],trigger[1],bin[0],opoint)
                if not opoint in skiplessS8MonitorData:
                    skiplessS8MonitorData[ opoint ] = {}
                if not bin[0] in skiplessS8MonitorData[ opoint ]:
                    skiplessS8MonitorData[ opoint ][ bin[0] ] = {}
                if not trigger[1] in skiplessS8MonitorData[ opoint ][ bin[0] ]:
                    skiplessS8MonitorData[ opoint ][ bin[0] ][ trigger[1] ] = []

                skiplessS8MonitorData[ opoint ][ bin[0] ][ trigger[1] ].append(\
                        run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            trigger_name = trigger[4],
                            step_postfix = step_postfix,
                            muon_pt = "6..",
                            data = "1",
                            input_files = dataTreeByName[ sample[1] ]))

#
# Run s8_monitor_input over QCD, taking skipevent/eventcount into consideration
#
skippedS8Monitor = {}
for opoint in operating_points:
    for bin in jet_bins:
        for trigger in trigger_list:
            for sample in qcd_datasets:
                deps = [ eventCountNodes[ sample[1] ],
                         # we automatically include deps to input files
                         #qcdTreeNodes[ sample[1]
                         luminositySumNode,
                         luminositySumByTrigger[ trigger[1] ],
                         eventCountNodes[ sample[1] ]
                       ]
    
                step_postfix = "-%s-%s-%s-%s" % ( sample[1], trigger[1], bin[0],opoint )
                currNode = run_monitor_input_helper( g,
                            step_postfix,
                            trigger_name = trigger[4],
                            skip_events =  ComputeSkipCount(  eventCountNodes[ sample[1] ],
                                                                 trigger[1],
                                                                 luminosityByTrigger,
                                                                 luminosityNodes.values() ),

                            event_count =  ComputeEventCount( eventCountNodes[ sample[1] ],
                                                                 luminositySumByTrigger[ trigger[1] ],
                                                                 luminosityNodes.values() ),
                            jet_pt = bin[1],
                            tag = opoint,
                            input_files = qcdTreeNodes[ sample[1] ],
                            muon_pt = "6..",
                            additional_dependencies = deps)
                nodekey = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
                if not nodekey in skippedS8Monitor:
                    skippedS8Monitor[nodekey] = []

                skippedS8Monitor[ nodekey ].append( currNode )


#
# root_qcd to stich the qcd files together
# then combine those with root_qcd [use the luminosity reported from Data running with Trigger DiJet20u]
#
class getLumi( LateBind ):
    def __init__(self, lumiNode):
        self.lumiNode = lumiNode
    def bind( self, edge ):
        print "lumiNode is %s" % str(self.lumiNode)
        return float( self.lumiNode.getValueFromOnlyOutputFile() )

def makeRootQCDFilenameFromInput( name ):
    pattern = "-qcd(.+?)-"
    print "searching %s and %s" % (pattern, name)
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
                print " got this newfile %s" % newFile
                if os.path.exists( newFile ):
                    os.unlink( newFile )
                os.symlink( oldFile, newFile )
                self.currCommand.append( newFile )
#       print "appended commandline: %s" % self.currComand
        return self.currCommand

def merge_with_root_qcd_helper( g, name , step_postfix,
                                inputNodes, luminosityNode ):
    mergeNode   =  Node( name = name ) 
    collectNode =  Node( name = "collect-" + name ) 
    # make the node
    g.addNode( mergeNode )
    g.addNode( collectNode )
    # Add the luminosity as a dependency
    g.addEdge( luminosityNode, collectNode, NullEdge() )
    # Add the previous S8 runs as a dependency
    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )
    
    #
    # Generate the command line
    #
    command = appendCommandLineSymlink( 
        currCommand = 
            ["root_qcd", getLumi( luminosityNode ), "merge.root" ],
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
            print "trigger bin is %s " % trigger[1]
            # only use trigger for the luminosity
            # we're using the qcd datasets for input files
            # first merge the skipless
        
            step_postfix = "-%s-%s-%s-noskip" % ( trigger[1], bin[0], opoint )
            mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                            inputNodes = skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ],
                            luminosityNode = luminositySumByTrigger[ trigger[ 1 ] ]
                        )
            mergekey = "%s-%s" %(bin[0], opoint) 
            if not mergekey in skiplessQCDMerge:
                skiplessQCDMerge[ mergekey ] = []
            skiplessQCDMerge[ mergekey ].append( mergeNode )

            # now merge the skipped
            step_postfix = "-%s-%s-%s" % ( trigger[1], bin[0], opoint )
            mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                            inputNodes = skippedS8Monitor[ "%s-%s-%s" % ( trigger[1], bin[0], opoint ) ],
                            luminosityNode = luminositySumByTrigger[ trigger[ 1 ] ]
                        )
            print "premerge is %s" % skippedQCDMerge
            if not mergekey in  skippedQCDMerge:
                print "creating mergekey %s for trigger %s" % ( mergekey, trigger[1] )
                skippedQCDMerge[ mergekey ] = []
                print "is is now %s" % skippedQCDMerge
            skippedQCDMerge[ mergekey ].append( mergeNode )

        # END TRIGGER LOOP, THIS IS OVER bin and opoint
        print "skipmerge %s " % skippedQCDMerge

        hadd_helper( g, mergekey + "skip", skippedQCDMerge[ mergekey ] )
        hadd_helper( g, mergekey + "noskip", skiplessQCDMerge[ mergekey ] )

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
