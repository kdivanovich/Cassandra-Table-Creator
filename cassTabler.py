import sys

db = (sys.argv[1])

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect(db)
	
	# test cases: 
	
argsLen = len(sys.argv)	
#print argsLen


# normal key,composite key, 

if (argsLen > 4 and sys.argv[2] == 'mount' and sys.argv[4] == 'single'):

	str = ''
	for n in range (1, (len(sys.argv)-5), 2 ):
		 str += (sys.argv[n+4]) + ' ' + (sys.argv[n+5]) + ', '
		 n += 1;
			
	var = session.execute(""" 
	CREATE TABLE IF NOT EXISTS """ + (sys.argv[3]) + """(
	
	""" + str + """
	
	PRIMARY KEY(""" +  (sys.argv[5]) + """) ,
	);
	""")
	
	print
	print 'Storage mounted.'
	
	
	
elif (argsLen > 4 and sys.argv[2] == 'mount' and sys.argv[4] == 'double'):

	str = ''
	for n in range (1, (len(sys.argv)-5), 2 ):
		 str += (sys.argv[n+4]) + ' ' + (sys.argv[n+5]) + ', '
		 n += 1;
			
	var = session.execute(""" 
	CREATE TABLE IF NOT EXISTS """ + (sys.argv[3]) + """(
	
	""" + str + """
	
	PRIMARY KEY(""" +  (sys.argv[5]) + """, """ + (sys.argv[7]) + """) ,
	);
	""")
	
	print
	print 'Storage mounted.'
	
	
elif (argsLen > 4 and sys.argv[2] == 'mount' and sys.argv[4] == 'triple'):

	str = ''
	for n in range (1, (len(sys.argv)-5), 2 ):
		 str += (sys.argv[n+4]) + ' ' + (sys.argv[n+5]) + ', '
		 n += 1;
			
	var = session.execute(""" 
	CREATE TABLE IF NOT EXISTS """ + (sys.argv[3]) + """(
	
	""" + str + """
	
	PRIMARY KEY(""" +  (sys.argv[5]) + """, """ + (sys.argv[7]) + """, """ + (sys.argv[9]) + """) ,
	);
	""")
	
	print
	print 'Storage mounted.'
	
	
	
	
elif (argsLen == 4  and sys.argv[2]) == 'dismount':

	mountedBool = 0;	# 0 = false, i.e. not mounted
	
	var = session.execute(""" 
	select columnfamily_name from system.schema_columnfamilies where keyspace_name = 'instagrim'
	""")

	for i in range (0, len(var)):
		# check if storage is mounted:
		if str(sys.argv[3]) in var[i]: mountedBool = 1 
		
	if mountedBool == 1:
		session.execute(""" 
		drop table instagrim."""+ str(sys.argv[3]) +""";
		""")		
		print
		print 'Storage dismounted.'
	else: 
		print
		print 'This storage is not mounted so it cannot be dismounted.'

	

	
elif (argsLen == 4 and sys.argv[2]) == 'check':
	var = session.execute(""" 
	select columnfamily_name from system.schema_columnfamilies where keyspace_name = 'instagrim'
	""")

	#used for the for loop
	listLen = len(var)
	#print listLen
	print

	#display all the tables i.e. storages mounted i.e. created and available	
	#print var[0]
	print 

	mountedBool = 0;	# 0 = false, i.e. not mounted

	for i in range (0, listLen):
		# check if storage is mounted:
		if str(sys.argv[3]) in var[i]: mountedBool = 1 
		

	if mountedBool == 1:
		print 'This storage is mounted.'
	else: print 'This storage is NOT mounted.'
	print
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

