'''
Created on Mar 30, 2012
Inserts entries in the value table of (Test)ValDb to make a 
timeseries of prices of traded assets on the GASCI and TTSE
exchanges.
(Test)ValDb is a MySQL dbase running on Mac OSX
owned by Pollards Et Filles Ltd.
@author: dpollard2
'''

import ValDbUtils as utl

if __name__ == '__main__':
    ''' Read CSV files for price data on asset and insert into (Test)ValDb
    '''
    import csv
    import sys
    import os
    import argparse as ap
    
#    Parse arguments
    pars= ap.ArgumentParser(description="Load GASCI-formatted data from csv file to ValDb")
    pars.add_argument('file',help='name of csv file in "dir" with GASCI/TTSE price history')
    pars.add_argument('dir',nargs='?',
                      default='/Users/dpollard2/Documents/Work/Pollards&Filles/Research/GASCI_Analysis/GASCI_Data/Jan2012/',
                      help='directory holding GASCI csv files with price histories')
    pars.add_argument('-ex --exchange',dest='exch',required=True,help='name of exchange e.g. GASCI')
    pars.add_argument('-db --dbase',dest='dbase',default='TestValDb',choices=['TestValDb','PeFValDb'],help='name of database')
    args= pars.parse_args()

    try:
        asst= utl.getAssets( args.dbase)
        exchange = utl.getExchanges( args.dbase)
        exID = exchange[(args.exch).upper()]
    except Exception, e:
        sys.exit('Exiting %s...Problem with Assets or Exchanges; %s' % (sys.argv[0], e))
    
#    check file available
    filename = args.dir + args.file
    print filename
    if not(os.access(filename, os.F_OK)):
        sys.exit("Exiting ... File %s not found" % filename)
        
#    connect to database
    conn = utl.connectMysql(args.dbase)
    csr = conn.cursor()

#    read csv file of prices and convert to list
#    INSERT prices into value table using a loop
    with open(filename, 'rU') as f:
        try:
            reader = csv.DictReader(f,fieldnames=('Ticker','Session','Date','Price'))
            numInserted= 0
            for row in reader:
                if (reader.line_num > 1):
                    if ((row['Price'].find('N/A') == -1) and (len(row['Price'])>0)):
                        hhmmss = str( asst[(exID,row['Ticker'])][1] )
                        tzon = asst[(exID,row['Ticker'])][2]
                        sqlStmt= "insert into value (assetID, datetime, valueTypeID, value) values (%d, '%s', 1, %s)" % (asst[(exID,row['Ticker'])][0], utl.reformatDate(row['Date'],hhmmss,tzon), row['Price'])
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
            
    