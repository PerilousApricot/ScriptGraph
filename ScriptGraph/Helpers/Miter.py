#!/usr/bin/env python

# miter - multiple iterator

# miter m
# m.add( 'foo', param1 = "a", param2 = "1" )
# m.add( 'foo', param1 = "b", param2 = "2", someval = "X" )

# for val in m.iterMany( 'param1', 'param2' ):
#   print val ## val = [ 'foo', { "param1" : "a", "param2" : "1" } ]
#             ## val = [ 'bar', { "param1" : "b", "param2" : "2", someval = "X" } ]

class Miter( type([]) ):
    def __init__( self, vals = None ):
        if not vals:
            self.vals = []
        else:
            self.vals = vals

    def getTupleFromKeys( self, val, keys ):
        retval = []
        for key in keys:
            retval.append( val[1][key] )
        return tuple( retval )

    def iterGrouped( self, *keys ):
        retval = {}
        for oneval in self.vals:
            if self.valMatchesKeys( oneval, keys ):
                key = self.getTupleFromKeys( oneval, keys )
                if not key in retval:
                    retval[ key ] = []
                retval[ key ].append( oneval )
        for onekey in retval:
            yield Miter( vals = retval[ onekey ] )

    def add( self, value, **tags ):
        self.vals.append([ value, tags ])
    
    def iterMany( self, *keys ):
        for oneval in self.vals:
            response = self.valMatchesKeys( oneval, keys )
            if response != None:
                yield response
    
    def iterManyValues( self, *keys ):
        for oneval in self.iterMany( *keys ):
            yield oneval[0]

    def getOne( self, **keys ):
        retval = []
        keyvals = keys.keys()
        for oneval in self.iterMany( *keyvals ):
            if self.valMatchesKeyValPairs( oneval, keys ):
                if retval:
                    raise RuntimeError, "One value was requested but we got more than one val: %s oldval: %s" % (oneval, retval)
                retval.append( oneval )
        return retval[0]

    def getOneValue( self, **keys ):
        return self.getOne( **keys )[0]

    def get( self, **keys ):
        retval = []
        keyvals = keys.keys()
        for oneval in self.iterMany( *keyvals ):
            if self.valMatchesKeyValPairs( oneval, keys ):
                retval.append( oneval )
        return retval

    def getValues( self, **keys ):
        retval = []
        for val in self.get( **keys ):
            retval.append(val[0])
        return retval
   
    def valMatchesKeys( self, val, keys ):
        for key in keys:
            if key not in val[1]:
                return None
        return val

    def valMatchesKeyValPairs( self, val, keys):
        for key in keys:
            if key not in val[1]:
                return None
            if val[1][key] != keys[key]:
                return None
        return val

    def __len__( self ):
        return len( self.vals )
    
if __name__ == '__main__':
    import unittest

    class testMiter( unittest.TestCase ):
        def testIter( self ):
            t = miter()
            for a in  ['foo','bar','baz']:
                for b in ['zyz','zyx']:
                    for c in ['aaa','bbb','123']:
                        if a == 'bar':
                            t.add( "%s%s%s" % (a,b,c), param1=a, param2=b,param3=c,optional=True )
                        else:
                            t.add( "%s%s%s" % (a,b,c), param1=a, param2=b,param3=c )
            
            test1 = []
            for val in t.iterManyValues( 'param1','param2','param3' ):
                test1.append( val )
            self.assertEqual( 18, len( test1 ) )

    unittest.main()



