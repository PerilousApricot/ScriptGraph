#!/usr/bin/env python
import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
from ScriptGraph.Graph.NullEdge import NullEdge

sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import genTree
from s8.MonitorInput import run_monitor_input_helper
from s8.RootQCD import merge_with_root_qcd_helper
from s8.Hadd import hadd_helper

from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
singleMonitorMiter = Miter()
baseWorkDir = "/uscms_data/d2/meloam/input39x2"
g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

operating_points = ["TCHEM", "TCHEL", "TCHET",
                    "TCHPT",  "TCHPM",  "TCHPL",
                    "SSVT",   "SSVM",   "SSVL",
                    "SSVHET", "SSVHEM",
                    "SSVHPT" ]
#operating_points = [ "TCHEL", "SSVHEM","TCHEM","SSVHPT" ]

operating_points = ["TCHEM", "SSVHEM"]

# operating_points = operating_points[0:1]

jet_bins = [ ["40to60", "40..60",   "35", "65"]
             ,["60to80", "60..80",  "55", "85" ]
             ,["80to140", "80..140","75", "145"]
             ,["140to", "140..", "135", "200"]
           ]

trigger_list_linked = { 'RUN2010B' : [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                                     [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" ],
                                     [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U"   ],
                                     [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U"  ] ],
                        'RUN2010A' : [ [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ] ]
                                  }
data_datasets = [ ['/BTau/Run2010B-Dec22ReReco_v1/AOD', 'RUN2010B'],
                  ['/BTau/Run2010A-Dec22ReReco_v1/AOD', 'RUN2010A']
]

trigger_list = [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" ],
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ],
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" ] 
]

trigger_map  = { 
    'hltdijet20u' :
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U", 'RUN2010B' ],
    'hltdijet20u-fullrange' :
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" , 'RUN2010B' ],
    'hltdijet30u' :
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" , 'RUN2010B' ],
    'hltjet10u' :
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" , 'RUN2010A' ],
    'hltjet20u' :
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" , 'RUN2010B' ] 
}

dataTriggerMiter = Miter()

dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "40..60" )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "40..60" )

dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltdijet20u-fullrange'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "60..80" )


dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "80..140" )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "80..140" )
dataTriggerMiter.add( trigger_map['hltdijet20u-fullrange'], bin = "80..140" )
dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "80..140" )


dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "140.." )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "140.." )
dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "140.." )
dataTriggerMiter.add( trigger_map['hltdijet30u'], bin = "140.." )

skiplessDataMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for tmap in dataTriggerMiter.iterGrouped( 'bin' ):
            if tmap.vals[0][1]['bin'] != bin[1]:
                continue

            for onetmap in tmap.iterMany():
                sample = onetmap[0][5]
                trigger = onetmap[0]
                step_postfix = "-%s-%s-%s-%s" % (sample,trigger[1],bin[0],opoint)
                monitor_node =\
                        run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            fileKey = sample,
                            trigger_name = trigger[4],
                            step_postfix = step_postfix,
                            muon_pt = "6..",
                            data = "1",
                            input_files = exports['data'].getValues( dataset = sample ))
                
                singleMonitorMiter.add( monitor_node,dataset = sample,
                                                trigger = trigger[1],
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "data" )

                skiplessDataMiter.add( monitor_node, dataset = sample,
                                                     trigger = trigger[1],
                                                     bin     = bin[0],
                                                     opoint  = opoint )

#
# Merge triggers
#
dataForWeights = {}
haddedDataMiter = Miter()
dataForPlots = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint) )
        haddedDataMiter.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )
        print "the opoint is %s" % opoint
        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data" )
        dataForWeights[ bin[0] ] = currNode
        if opoint == "TCHEM":
            dataForPlots.add( currNode, bin = bin[0] )

#
# Merge triggers - run2010B
#
dataForWeights_rangeb = {}
haddedDataMiter_rangeb = Miter()
dataForPlots_rangeb = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data-rangeb" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint,
                                        dataset = 'RUN2010B') )
        haddedDataMiter_rangeb.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )
        print "the opoint is %s" % opoint
        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data-rangeb" )
        dataForWeights_rangeb[ bin[0] ] = currNode
        if opoint == "TCHEM":
            dataForPlots_rangeb.add( currNode, bin = bin[0] )

#
# Merge triggers - run2010A
#
dataForWeights_rangea = {}
haddedDataMiter_rangea = Miter()
dataForPlots_rangea = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data-rangea" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint,
                                        dataset = 'RUN2010A') )
        haddedDataMiter_rangea.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )
        print "the opoint is %s" % opoint
        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data-rangea" )
        dataForWeights_rangea[ bin[0] ] = currNode
        if opoint == "TCHEM":
            dataForPlots_rangea.add( currNode, bin = bin[0] )




#
# Merge Bins
#
haddedDataFinalMiter = Miter()
for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-data-final" % (opoint.vals[0][1]['opoint'],), 
                                skiplessDataMiter.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedDataFinalMiter.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_data-final" )

#
# Merge Bins - rangea
#
haddedDataFinalMiter_rangea = Miter()
for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-data-final-rangea" % (opoint.vals[0][1]['opoint'],), 
                                skiplessDataMiter.getValues( opoint = opoint.vals[0][1]['opoint'],
                                                             dataset = 'RUN2010A') )
    haddedDataFinalMiter_rangea.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_data-final-rangea" )
    if opoint.vals[0][1]['opoint'] == "TCHEM":
        dataForPlots_rangea.add( currNode, bin = "all" )


#
# Merge Bins - rangeb
#
haddedDataFinalMiter_rangeb = Miter()
for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-data-final-rangeb" % (opoint.vals[0][1]['opoint'],), 
                                skiplessDataMiter.getValues( opoint = opoint.vals[0][1]['opoint'],
                                                             dataset = 'RUN2010B') )
    haddedDataFinalMiter_rangeb.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_data-final-rangeb" )
    print "opoint is %s" % opoint.vals[0][1]['opoint']
    if opoint.vals[0][1]['opoint'] == "TCHEM":
        print "GOTCHEM"
        dataForPlots_rangeb.add( currNode, bin = "all" )




#################
###### At this point, we have all we need for data
#################


#
# Run s8_monitor_input over QCD, for the input to reweighting (so we don't care about tag
# or trigger, only the bin)
#
preweightMiter   = Miter()
for dataset in exports['qcd'].iterMany():
    for bin in jet_bins:
        step_postfix = "-%s-%s-preweight" % (dataset[1]['dataset'],bin[0])
        currNode = run_monitor_input_helper( g,
                    jet_pt = bin[1],
#                    tag = operating_,
                    fileKey = dataset[1]['dataset'],
#                        trigger_name = "HLT_BTagMu_DiJet10U",
                    step_postfix = step_postfix,
                    muon_pt = "6..",
                    input_files = dataset[0] )
        preweightMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'] )

#
# Merge the pthats
#

mcForWeights = {}
preweightMergeMiter = Miter()
mcForPlots = Miter()
mcForPlots_rangea = Miter()
mcForPlots_rangeb = Miter()
for currmerge in preweightMiter.iterGrouped( 'bin' ):
    step_postfix = "-%s-preweight" % ( currmerge.vals[0][1]['bin'], )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    preweightMergeMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'] )
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "skipless_root_qcd" )
    mcForWeights[ currmerge.vals[0][1]['bin'] ] = mergeNode
#    mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
# Calculate the weights (on a bin-only basis)
#
weightMiter = Miter()
binToWeight = {}
binToWeightNode = {}
print "data %s mc %s" % (dataForWeights, mcForWeights )
for bin in jet_bins:
    step_key    = "-%s" % ( bin[0], )
    step_postfix= step_key
    collectNode = Node( name = "collect-reweight" + step_postfix )
    weightNode  = Node( name = "weight" + step_postfix )
    reweighNode = Node( name = "reweighed" + step_postfix )

    dataNode    = dataForWeights[ bin[0] ]
    mcNode      = mcForWeights[ bin[0] ]
    # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
    calcWeightEdge = LocalScriptEdge(
                    name = "calcweight" +  step_postfix,
                    command = ["root_trigger_weights",
                                    bin[1],
                                    #"-3..3",
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

    binToWeight[ bin[0] ] = calcWeightEdge
    binToWeightNode[ bin[0] ] = weightNode

#
# Calculate the weights (on a bin-only basis) - for systematics - a
#
weightMiter_rangea = Miter()
binToWeight_rangea = {}
binToWeightNode_rangea = {}
for bin in jet_bins:
    step_key    = "-%s" % ( bin[0], )
    step_postfix= step_key + "-a"
    collectNode = Node( name = "collect-reweight-a" + step_postfix )
    weightNode  = Node( name = "weight-a" + step_postfix )
    reweighNode = Node( name = "reweighed-a" + step_postfix )

    dataNode    = dataForWeights_rangea[ bin[0] ]
    mcNode      = mcForWeights[ bin[0] ]
    # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
    calcWeightEdge = LocalScriptEdge(
                    name = "calcweight-rangea" +  step_postfix,
                    command = ["root_trigger_weights",
                                    bin[1],
                                    #"-3..3",
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

    binToWeight_rangea[ bin[0] ] = calcWeightEdge
    binToWeightNode_rangea[ bin[0] ] = weightNode


#
# Calculate the weights (on a bin-only basis) - for systematics - b
#
weightMiter_rangeb = Miter()
binToWeight_rangeb = {}
binToWeightNode_rangeb = {}
for bin in jet_bins:
    step_key    = "-%s" % ( bin[0], )
    step_postfix= step_key + "-b"
    collectNode = Node( name = "collect-reweight-a" + step_postfix )
    weightNode  = Node( name = "weight-a" + step_postfix )
    reweighNode = Node( name = "reweighed-a" + step_postfix )

    dataNode    = dataForWeights_rangeb[ bin[0] ]
    mcNode      = mcForWeights[ bin[0] ]
    # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
    calcWeightEdge = LocalScriptEdge(
                    name = "calcweight-rangea" +  step_postfix,
                    command = ["root_trigger_weights",
                                    bin[1],
                                    #"-3..3",
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

    binToWeight_rangeb[ bin[0] ] = calcWeightEdge
    binToWeightNode_rangeb[ bin[0] ] = weightNode


reweightMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight[ bin[0] ] ) )
            reweightMiter.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )

reweightMiterRangeLowPV = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight-lowpv2" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight[ bin[0] ] ),
                        npv = "1..2")
            reweightMiterRangeLowPV.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )


reweightMiterRangeHighPV = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight-highpv" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight[ bin[0] ] ),
                        npv = "2..")
            reweightMiterRangeHighPV.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )


# reweight with range a
reweightMiter_rangea = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight-rangea" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s-rangea" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode_rangea[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight_rangea[ bin[0] ] ) )
            reweightMiter_rangea.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )
            
# reweight with range b
reweightMiter_rangeb = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight-rangeb" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s-rangeb" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode_rangeb[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight_rangeb[ bin[0] ] ) )
            reweightMiter_rangeb.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )
            


#
# Merge the pthats
#

reweightMergePthatMiter = Miter()
for currmerge in reweightMiter.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight" % ( currmerge.vals[0][1]['bin'],currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )


#
# Merge the pthats - low PV
#

reweightMergePthatMiterLowPV = Miter()
for currmerge in reweightMiterRangeLowPV.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight-lowpv" % ( currmerge.vals[0][1]['bin'],currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiterLowPV.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )

#
# Merge the pthats - high PV
#

reweightMergePthatMiterHighPV = Miter()
for currmerge in reweightMiterRangeHighPV.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight-highpv" % ( currmerge.vals[0][1]['bin'],currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiterHighPV.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
#
# Merge Bins low PV
#
haddedMCFinalMiterLowPV = Miter()
for opoint in reweightMergePthatMiterLowPV.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin-lowpv" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiterLowPV.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiterLowPV.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final-lowpv" )
#
# Merge Bins high PV
#
haddedMCFinalMiterHighPV = Miter()
for opoint in reweightMergePthatMiterHighPV.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin-highpv" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiterHighPV.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiterHighPV.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final-highpv" )




#
#
# Merge Bins
#
haddedMCFinalMiter = Miter()
for opoint in reweightMergePthatMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiter.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiter.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final" )


#
# Merge the pthats -rangea
#

reweightMergePthatMiter_rangea = Miter()
for currmerge in reweightMiter_rangea.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight-rangea" % ( currmerge.vals[0][1]['bin'],
                                                currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiter_rangea.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots_rangea.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
#
# Merge Bins -rangea
#
haddedMCFinalMiter_rangea = Miter()
for opoint in reweightMergePthatMiter_rangea.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin-rangea" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiter_rangea.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiter_rangea.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final-rangea" )


#
# Merge the pthats -rangeb
#

reweightMergePthatMiter_rangeb = Miter()
for currmerge in reweightMiter_rangeb.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight-rangeb" % ( currmerge.vals[0][1]['bin'],
                                                currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiter_rangeb.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots_rangeb.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
#
# Merge Bins -rangeb
#
haddedMCFinalMiter_rangeb = Miter()
for opoint in reweightMergePthatMiter_rangeb.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin-rangeb" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiter_rangeb.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiter_rangeb.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final-rangeb" )




#
# Rerun over QCD with weights
#

#preweightMiter   = Miter()
#for dataset in exports['qcd'].iterMany():
#    for bin in jet_bins:
#        step_postfix = "-%s-%s-weighter" % (dataset[1]['dataset'],bin[0])
#        currNode = run_monitor_input_helper( g,
#                    jet_pt = bin[1],
##                    tag = operating_,
#                    fileKey = dataset[1]['dataset'],
##                        trigger_name = "HLT_BTagMu_DiJet10U",
#                    step_postfix = step_postfix,
#                    muon_pt = "6..",
#                    input_files = dataset[0] )
#        preweightMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'] )
#
#
# Some website stuff
#
allPages = Miter()
webRoot  = "/afs/fnal.gov/files/home/room3/meloam/public_html/s8/39x/"
#webRoot  = "/uscms_data/d2/meloam/input39x2/edges/plots/"
httpRoot = "http://home.fnal.gov/~meloam/s8/39x/"


#
# Generate single monitoring plots
#
#singleMonitorImages = Miter()
#singleMonitorTarget = Node( name = "singleMonitorTarget" )
#singleMonitorAbs = os.path.join( webRoot, 'singleMonitor' )
#singleMonitorRel  = 'singleMonitor'
#
#g.addNode( singleMonitorTarget )
#
#count = 0
#singleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/SingleComparison.C'
#
#def getFileNameStub( args ):
#    return args['node'].getOnlyFile()
#
#singleMonitorPlotList = Miter()
#for onenode in singleMonitorMiter.iterMany():
#
#    print "miter is %s" % onenode[1]
#    count += 1
#    subMonitorAbs = os.path.join( singleMonitorAbs )#, onenode[1]['opoint'], onenode[1]['bin'] )
#    subMonitorRel = os.path.join( singleMonitorRel )#, onenode[1]['opoint'], onenode[1]['bin'] )
#    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt.png", "Njet_{pt}" ],
#                     [ "MonitorAnalyzer/n/njet_eta","-njeteta.png", "Njet_{eta}"],
#                     [ "MonitorAnalyzer/generic/pvs","-pvs.png", "N_{pv}" ] ]
#    fileDesc = onenode[1]['type']
#    if 'dataset' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['dataset']
#    if 'trigger' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['trigger']
#
#    for oneMonitor in monitorNames:
#        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
#
#        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s" % ( fileDesc, oneMonitor[1] ) )
#        targetFileNameRel = os.path.join( subMonitorRel, "%s%s" % ( fileDesc, oneMonitor[1] ) )
#        getImage = LocalScriptEdge(
#                            name = "extract-monitor-%s-%s" % (count, oneMonitor[1]) , 
#                            command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\")'" % \
#                                ( singleComparisonMacro, '%s', 
#                                    oneMonitor[0], targetFileNameAbs, oneMonitor[2] ),
#                                  [BindFunction( func = getFileNameStub,
#                                                 args = { 'node': onenode[0] } )] ),
#                            output=targetFileNameAbs, 
#                            noEmptyFiles=True)
#        getImage.setWorkDir( subMonitorAbs )
#        g.addEdge( onenode[0], singleMonitorTarget, getImage )
#
#        singleMonitorPlotList.add( targetFileNameRel, opoint = onenode[1]['opoint'], 
#                                                      bin    = onenode[1]['bin'],
#                                                      trigger= onenode[1]['trigger'] if 'trigger' in onenode[1] else "none" )
#
#

def getFileNameStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return args['node'].getOnlyFile()
    except:
        print "failed at %s" % args['node'].getName()
        raise


s8macro = "/uscms/home/meloam/s8/CMSSW_3_9_8_patch1/src/RecoBTag/PerformanceMeasurements/test/S8Solver/run_s8.C"
scaleFactors = Miter()

for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiter ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-close-%s" % opoint )
    solution= Node( name = "s8volve-close-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-closure-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",1)' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "closure", opoint=opoint )

# closure for highpv
for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiterHighPV ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-close-high-%s" % opoint )
    solution= Node( name = "s8volve-close-high-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-closure-highpv-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",1)' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "closure", opoint=opoint )

# closure for lowpv

for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiterLowPV ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-close-lowpv-%s" % opoint )
    solution= Node( name = "s8volve-close-lowpv-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-closure-lowpv-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",1)' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "closure", opoint=opoint )


def getFileNameForClosureStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return os.path.join( os.path.dirname( args['node'].getOnlyFile() ), "s8.root" )
    except:
        print "failed at %s" % args['node'].getName()
        raise
#
#for closure in scaleFactors.iterManyMatchingConditions( type = "closure" ):
#    print "got closure %s" % closure[1]['opoint']
#    opoint = closure[1]['opoint']
#    closureNode = Node( name = "systeamatics-closure-%s" % opoint )
#    g.addNode( closureNode )
#    runs8edge = LocalScriptEdge(
#                        name = "systematics-closure--%s" % ( opoint) , 
#                        command = BindSubstitutes("rm mc.root ; ln -s %s mc.root ; root_systematics output.root %s mc.root",
#                              [BindFunction( func = getFileNameForClosureStub,
#                                             args = { 'node': closure[0].getChild() } ),
#                               BindFunction( func = getFileNameForClosureStub,
#                                             args = { 'node': closure[0].getChild() } )] ),
#                        output="output.root", 
#                        noEmptyFiles=True)
#
#    g.addEdge( closure[0].getChild(), closureNode, runs8edge )
#





for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiter ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-%s" % opoint )
    solution= Node( name = "s8volve-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "nominal" )

### NO PV
for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiterLowPV ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-lowpv-%s" % opoint )
    solution= Node( name = "s8volve-lovpv-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-%s-lowpv" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "nominal" )

### HIGH PV
for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiterHighPV ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-highpv-%s" % opoint )
    solution= Node( name = "s8volve-%s-highpv" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-%s-highpv" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "nominal" )





# BETTERAPPROACH
for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiter_rangeb ):
    print "zipping1-new-try2 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangeb-newweight2-%s" % opoint )
    solution= Node( name = "s8volve-rangeb-newweigh2t-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangeb-new-2-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangeb-newweight2" )

for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiter_rangea ):
    print "zipping-newwgith-rangea2 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangea-newweight-2-%s" % opoint )
    solution= Node( name = "s8volve-rangea-newweight-2-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangea-new-2-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangea-newweight2" )
# NAIVE APPROACH

for (onedata, onemc) in haddedDataFinalMiter_rangeb.zip( haddedMCFinalMiter_rangeb ):
    print "zipping1-new %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangeb-newweight-%s" % opoint )
    solution= Node( name = "s8volve-rangeb-newweight-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangeb-new-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangeb-new" )

for (onedata, onemc) in haddedDataFinalMiter_rangea.zip( haddedMCFinalMiter_rangea ):
    print "zipping-newwgith-rangea %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangea-newweight-%s" % opoint )
    solution= Node( name = "s8volve-rangea-newweight-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangea-new-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangea-new" )

# OLD WEIGHT
for (onedata, onemc) in haddedDataFinalMiter_rangeb.zip( haddedMCFinalMiter ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangeb-oldweight-%s" % opoint )
    solution= Node( name = "s8volve-rangeb-oldweight-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangeb-oldweight-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangeb-old" )

for (onedata, onemc) in haddedDataFinalMiter_rangea.zip( haddedMCFinalMiter ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-rangea-oldweight-%s" % opoint )
    solution= Node( name = "s8volve-rangea-oldweight-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-rangea-oldweight-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "rangea-old" )

def getS8RootFile( args ):
    return args['node'].getWorkDir() + "/s8.root"

solcount = 0
for s8solution in scaleFactors.iterMany():
    target = s8solution[0].getChild()
    solname = s8solution[0].getName() + "-scale"
    solcount += 1
    output = Node( name = "scale_factor_%s" % solcount )
    runScale = LocalScriptEdge(
                    name = solname,
                    command = BindSubstitutes("root_scale %s &> scale.txt" ,
                                    [BindFunction( func = getS8RootFile,
                                                   args = { 'node': s8solution[0] } ) ] ),
                    output = "scale.txt",
                    noEmptyFiles = True )
    g.addNode( output )
    g.addEdge( target,output, runScale )

mcVsDataCollect = Node( name = "mcVsData-collect" )
mcVsDataWebpage = Node( name = "mcVsData-page" )
doubleMonitorImages = Miter()
doubleMonitorTarget = Node( name = "doubleMonitorTarget" )
doubleMonitorAbs = os.path.join( webRoot, 'doubleMonitor' )
doubleMonitorRel  = 'doubleMonitor'

g.addNode( doubleMonitorTarget )

count = 0
doubleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/DoubleComparison.C'
doubleMonitorPlotList = Miter()

# get together all the different plots we want to compare
allDoublePlots = []
for (onedata, onemc) in dataForPlots.zip( mcForPlots ):
    allDoublePlots.append( [ onedata, onemc, "Data", "Simulation", "complete", onedata[1]['bin']] )

print "rangea %s" % dataForPlots_rangea.vals
print "rangeb %s" % dataForPlots_rangeb.vals


for (onedata, onemc) in dataForPlots_rangea.zip( dataForPlots_rangeb ):
    allDoublePlots.append( [ onedata, onemc, "RangeA", "RangeB", "PUcompare", onedata[1]['bin']] )


 
for (onedata, onemc, dataLabel, mcLabel, plotLabel, plotBin) in allDoublePlots:
    print "zipping %s %s" % (onedata[1], onemc[1] )

    
    subMonitorAbs = os.path.join( doubleMonitorAbs, "%s-%s" % (plotBin, plotLabel) )
    subMonitorRel = os.path.join( doubleMonitorRel, "%s-%s" % (plotBin, plotLabel) )
    minbin = 35
    maxbin = 205
    if 'bin' in onedata[1]:
        for onebin in jet_bins:
            if onebin[0] == onedata[1]['bin']:
                minbin = onebin[2]
                maxbin = onebin[3]

    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt", "Njet_{pt}" ],
                     [ "MonitorAnalyzer/n/njet_eta","-njeteta", "Njet_{eta}"],
                     [ "MonitorAnalyzer/n/npvs","-pvs", "N_{pv}" ] ]
    fileDesc = "mcVsdata-reweight"
#    if 'dataset' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['dataset']
#    if 'trigger' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['trigger']

    for oneMonitor in monitorNames:
        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
        count += 1
        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRel = os.path.join( subMonitorRel, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbs = os.path.join( subMonitorAbs, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRel = os.path.join( subMonitorRel, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameAbsPdf = os.path.join( subMonitorAbs, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRelPdf = os.path.join( subMonitorRel, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbsPdf = os.path.join( subMonitorAbs, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRelPdf = os.path.join( subMonitorRel, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )

        print "Zipping %s to %s" % (onedata[0].getName(), onemc[0].getName() )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s" % (count, oneMonitor[1]) , 
                # big command #
                            command = \
                BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s,\"%s\",\"%s\")'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbs,targetFileNameRatioAbs, oneMonitor[2], minbin, maxbin,
                                    dataLabel, mcLabel),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        collectNode = Node( "collect-double-%s" % count )
        g.addNode( collectNode )
        g.addEdge( onedata[0], collectNode, NullEdge() )
        g.addEdge( onemc[0], collectNode, NullEdge() )

        g.addEdge( collectNode, doubleMonitorTarget, getImage )

        doubleMonitorPlotList.add( targetFileNameRel,# opoint = onedata[1]['opoint'], 
                                                     #b:in    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s-pdf" % (count, oneMonitor[1]) , 
                            # big command #
                            command = \
                BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s,\"%s\",\"%s\")'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbsPdf,targetFileNameRatioAbsPdf, oneMonitor[2], minbin, maxbin,
                                    dataLabel, mcLabel),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        collectNode = Node( "collect-double-%s-pdf" % count )
        g.addNode( collectNode )
        g.addEdge( onedata[0], collectNode, NullEdge() )
        g.addEdge( onemc[0], collectNode, NullEdge() )

        g.addEdge( collectNode, doubleMonitorTarget, getImage )

        doubleMonitorPlotList.add( targetFileNameRel,# opoint = onedata[1]['opoint'], 
                                                     #b:in    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )


output = "<html><head><title>dijet10 exercise</title></head><body><h1>dijet10U exercise</h1><hr />"
output += "<p>MC is cut on the HLT_BTagMu_DiJet10U trigger, Data is Jet10U+Jet20U</p><hr />"
#for onebin in doubleMonitorPlotList.iterGrouped( 'bin' ):
#    currbin = onebin.vals[0][1]['bin']
#    output += "<h2> bin %s </h2><hr />" % currbin
for oneop in doubleMonitorPlotList.iterGrouped( 'opoint' ):
#    currop  = oneop.vals[0][1]['opoint']
    currop = "test"
    output += "<h3>Operating point %s</h3>" % currop
    for onemon in oneop.iterMany():
        output += "<h4>%s</h4>\n" % onemon[1]['type']
        output += '<img src="%s" /> <img src="%s" />\n' % (onemon[0], onemon[1]['ratiorel'])
output += "</body>"

try:
    print output
    handle = open( webRoot + "/dump.html", "w+" )
    handle.write(output)
    handle.close()
except:
    print "Couldn't do output"


def getGraph():
    global g
    return g
#for opoint in operating_points:
#    for bin in jet_bins:
#        for sample in qcd_datasets:
#            for trigger in trigger_list:
#                step_postfix = "-%s-%s-%s-%s-noskip" % (sample[1],trigger[1],bin[0],opoint)
#                if not opoint in skiplessS8Monitor:
#                    skiplessS8Monitor[ opoint ] = {}
#                if not bin[0] in skiplessS8Monitor[ opoint ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ] = {}
#                if not trigger[1] in skiplessS8Monitor[ opoint ][ bin[0] ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ] = []
#
#                currNode = run_monitor_input_helper( g,
#                            jet_pt = bin[1],
#                            tag = opoint,
#                            fileKey = sample[1],
#                            trigger_name = trigger[4],
#                            step_postfix = step_postfix,
#                            muon_pt = "6..",
#                            input_files = qcdTreeMiter.getOneValue( dataset =  sample[1] ),
#                            simulate_trigger = triggers_to_simulate[ trigger[1] ])
#                skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ].append( currNode )
#                singleMonitorMiter.add( currNode,dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint,
#                                                njetpt  = True,
#                                                njeteta = True,
#                                                privert = True,
#                                                type    = "skipless" )
# 
#
#                skiplessQCDMiter.add( currNode, dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint )
# 
#
#
#
