'''
    Storage : A Class which have 5 atribute 
    such as key, val, writeTS, readTS, dan version
'''

class Storage:
    data = []
    
    def __init__(self, length):
        '''
            Initiate storage with 5 element of data
            such as key, val, writeTS, readTS, dan version
        '''
        for i in range(length):
            self.data.append([i,0,0,0,0])

    def getLatestVersion(self,key):
        '''
            Method to return new version of a element
        '''
        version = 0
        for i in self.data:
            current_Key = i.key
            current_Ver = i.version
            if(current_Key == key):
                if(current_Ver > version):
                    version = current_Ver
        return version

    def getHighestwriteTS(self,timestamp,key):
        '''
            Method to find idx that denote the version of Q
            whose write timestamp is the largest write timestamp
            less than or equal to TS(Ti)
        '''
        index = 0
        wts = self.data[0].writeTS
        for i in self.data:
            current_Key = i.key
            current_WTS = i.writeTS
            if(key==current_Key):
                if(current_WTS>wts):
                    if(current_WTS<=timestamp): # Qi terdeteksi
                        index = i
                        wts = current_WTS
        return index


    def addElement(self,key,val,rTS,wTS,version):
        '''
            Methof to add new element to data
        '''
        self.data.append([key,val,rTS,wTS,version])
    
    def showStorage(self):
        '''
            Method to show storage information
        '''
        for i in self.data:
            print("----------------------------")
            print("Read R-TS: ",i.readTS)
            print("Read W-TS: ",i.writeTS)
            print("Version: ",i.version)
            print("Key: ",i.key)
            print("Value: ",i.val)
            print()