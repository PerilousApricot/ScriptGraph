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
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Graph.GeneratePageEdge import GeneratePageEdge


import ScriptGraph.Graph.CondorScriptEdge as CondorScriptEdge
g = Graph.Graph()
baseWorkDir = "/uscms_data/d2/meloam/s8workflow"
g.setWorkDir( baseWorkDir )
nullInput = Node( name = "nullInput-genTree" )
g.addNode( nullInput )

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

dataTreeMiter  = Miter()
luminosityMiter     = Miter()
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

def getDatasets( ):
    global qcdTreeMiter, dataTreeMiter
    return { "qcd" : qcdTreeMiter,
             "data": dataTreeMiter }

def getGraph( ):
    global g
    return g
