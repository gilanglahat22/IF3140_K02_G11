from storage import *

class MVCC :
    def __init__(self):
        '''
            Initiate MVCC class
        '''
        return

    def read(self, key, nilai, storage, timestamp):
        '''
            Method to implementing MVCC read rule's
        '''

        index = storage.getHighestwriteTS(timestamp,key)
        nilai = storage.data[index].val
        if(storage.data[index].readTS <= timestamp): # R-TS(Qk) <= TS(Ti)
            storage.data[index].readTS = timestamp
        return True
    
    def write(self, key, nilai, storage, timestamp):
        '''
            Method to implementing MVCC write rule's
        '''
        index = storage.getHighestwriteTS(timestamp,key)
        if(storage.data[index].readTS <= timestamp):
            if(storage.data[index].writeTS != timestamp):
                # Create a new version Qi of Q
                latestVer = storage.getLatestVersion(key) + 1
                storage.addElement(key,nilai,timestamp,timestamp,latestVer)
            else: # TS(Ti) = W-TS(Qk)
                # Overwrite konten Qk
                storage.data[index].val = nilai
            return True
        else: # R-TS(Qk) > TS(Ti)
            # Rollback Ti
            return False