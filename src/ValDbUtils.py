'''
Created on Aug 18, 2012
Python module with routines for PeF_ValDb access and
data management.
Owned by Pollards Et Filles Ltd.
@author: dpollard2
'''


def connectMysql(dbname):
    '''Make and return connection to Mysql dbase dbname '''
#    import sys
    import MySQLdb
    
    try:
        conn = MySQLdb.connect (db = dbname,
                                host = "localhost",
                                port = 3307,
                                user = "dpollard2",
                                passwd = "connect2mysql")
    except Exception, e:
        print "Cannot connect to server! %s. Re-throwing" % e
        raise
    else:
        print "Connected to %s" % dbname
        return conn


def getAssets(dbname):
    '''Get list of assets, ids, close times and timezones and return as dict
    keyed on tuple (exchangeID, assetID) '''
    
    connA = connectMysql(dbname)
    asst = dict()
    csr = connA.cursor()
    try:
        csr.execute("""select e.exchangeID, a.ticker, a.assetID, a.close, e.timezone
        from asset a, exchange e 
        where a.exchangeID = e.exchangeID
        order by e.exchangeID, a.ticker""")
#        print "No. rows selected: %d" % csr.rowcount
        rows = csr.fetchall()
        for r in rows:
            asst[(int(r[0]),r[1])] = (int(r[2]), r[3], r[4])
    except Exception, e:
        print "Error during asset list selection: %s" % e
        raise
    finally:
        csr.close()
#        print "Asset dictionary: ", asst  #print dictionary
        return asst


def getExchanges(dbname):
    '''Get list of exchangeIDs return as dict keyed on exchange(name)'''
    
    connA = connectMysql(dbname)
    exch = dict()
    csr = connA.cursor()
    try:
        csr.execute("""select exchange, exchangeID from exchange order by exchangeID""")
#        print "No. rows selected: %d" % csr.rowcount
        rows = csr.fetchall()
        for r in rows:
            exch[r[0]] = int(r[1])
    except Exception, e:
        print "Error during exchange list selection: %s" % e
    finally:
        csr.close()
#        print "Exchange dictionary: ", exch  #print dictionary
        return exch
    
    
def reformatDate( ddmmmyy, hhmmss, tzone):
    ''' change date format from dd-mmm-yy to yyyy-mm-dd hh:mm converted to UTC'''
    import datetime
    import pytz
    dt= datetime.datetime.strptime(ddmmmyy + " " + hhmmss,'%d-%b-%y %H:%M:%S')
#    utc = pytz.utc
    locTZ = pytz.timezone(tzone)
    locDT = locTZ.localize(dt)
    utcDT = locDT.astimezone(pytz.utc)
    return utcDT.strftime('%Y-%m-%d %H:%M')
