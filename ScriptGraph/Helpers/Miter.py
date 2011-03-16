#!/usr/bin/env python

# miter - multiple iterator

# miter m
# m.add( 'foo', param1 = "a", param2 = "1" )
# m.add( 'foo', param1 = "b", param2 = "2", someval = "X" )

# for val in m.iterMany( 'param1', 'param2' ):
#	print val ## val = [ 'foo', { "param1" : "a", "param2" : "1" } ]
#             ## val = [ 'bar', { "param1" : "b", "param2" : "2", someval = "X" } ]

class miter( type([]) ):
	def __init__( self, vals = None ):
		if not vals:
			self.vals = []
		else:
			self.vals = vals

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

	def valMatchesKeys( self, val, keys ):
		for key in keys:
			if key not in val[1]:
				return None
		return val
	
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



