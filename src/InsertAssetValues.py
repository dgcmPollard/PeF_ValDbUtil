'''
Created on Mar 30, 2012
Inserts entries in the value table of (Test)ValDb to make a 
timeseries of prices of traded assets on the GASCI and TTSE
exchanges.
(Test)ValDb is a MySQL dbase running on Mac OSX
owned by Pollards Et Filles Ltd.
@author: dpollard2
'''


def connectMysql(dbname):
    '''Make and return connection to Mysql dbase dbname '''
    import sys
    import MySQLdb
    
    try:
        conn = MySQLdb.connect (db = dbname,
                                host = "localhost",
                                port = 3307,
                                user = "dpollard2",
                                passwd = "connect2mysql")
    except Exception, e:
        print "Cannot connect to server! %s. Exiting" % e
        sys.exit()
    else:
        print "Connected to %s" % dbname
        return conn


def getAssets(dbname):
    '''Get list of assets, ids, close times and timezones and return as dict'''
    
    connA = connectMysql(dbname)
    asst = dict()
    csr = connA.cursor()
    try:
#        csr.execute("select ticker, assetID from asset order by ticker")
        csr.execute("""select a.ticker, a.assetID, a.close, e.timezone
        from asset a, exchange e 
        where a.exchangeID = e.exchangeID
        order by a.ticker""")
#        print "No. rows selected: %d" % csr.rowcount
        rows = csr.fetchall()
        for r in rows:
            asst[r[0]] = (int(r[1]), r[2], r[3])
    except Exception, e:
        print "Error during asset list selection: %s" % e
    finally:
        csr.close()
#        print "Asset dictionary: ", asst  #print dictionary
        return asst
    
    
def reformatDate( ddmmmyy, hhmmss, tzone):
    ''' change date format from dd-mmm-yy to yyyy-mm-dd hh:mm:ss converted to UTC'''
    import datetime
    import pytz
    dt= datetime.datetime.strptime(ddmmmyy + " " + hhmmss,'%d-%b-%y %H:%M:%S')
#    utc = pytz.utc
    locTZ = pytz.timezone(tzone)
    locDT = locTZ.localize(dt)
    utcDT = locDT.astimezone(pytz.utc)
    return utcDT.strftime('%Y-%m-%d %H:%M')




if __name__ == '__main__':
    ''' Read CSV files for price data on asset and insert into (Test)ValDb
    '''
    import csv
    import sys
    import os
    import argparse as ap
    
#    Parse arguments
    pars= ap.ArgumentParser(description="Load GASCI data from csv file to ValDb")
    pars.add_argument('file',help='name of csv file in "dir" with GASCI/TTSE price history')
    pars.add_argument('dir',nargs='?',
                      default='/Users/dpollard2/Documents/Work/Pollards&Filles/Research/GASCI_Analysis/GASCI_Data/Jan2012/',
                      help='directory holding GASCI csv files with price histories')
    pars.add_argument('-db --dbase',dest='dbase',default='TestValDb',choices=['TestValDb','PeFValDb'],help='name of database')
    args= pars.parse_args()

    asst= getAssets( args.dbase)
    
#    check file available
    filename = args.dir + args.file
    print filename
    if not(os.access(filename, os.F_OK)):
        sys.exit("Exiting ... File %s not found" % filename)
        
#    connect to database
    conn = connectMysql(args.dbase)
    csr = conn.cursor()

#    read csv file of prices and convert to list
#    INSERT prices into value table using a loop
    with open(filename, 'rU') as f:
        try:
            reader = csv.DictReader(f,fieldnames=('Ticker','Session','Date','Price'))
            numInserted= 0
            for row in reader:
                if (reader.line_num > 1):
                    if (row['Price'].find('N/A') == -1):
                        hhmmss = str( asst[row['Ticker']][1] )
                        tzon = asst[row['Ticker']][2]
                        sqlStmt= "insert into value (assetID, datetime, valueTypeID, value) values (%d, '%s', 1, %s)" % (asst[row['Ticker']][0], reformatDate(row['Date'],hhmmss,tzon), row['Price'])
                        print sqlStmt
                        csr.execute(sqlStmt)
                        numInserted += 1
#            print "Number of rows: %d" % (reader.line_num-1)
        except Exception, e:
            sys.exit('Exiting from data insertion...File %s, line %d, %s' % (filename, reader.line_num, e))
        finally:
            csr.close()
            conn.commit()
            print "Number of rows added: %d" % (numInserted)
            f.close()
            
    