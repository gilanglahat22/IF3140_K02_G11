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
            current_Key = i[0]
            current_Ver = i[4]
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
        wts = self.data[0][3]
        for i in self.data:
            current_Key = i[0]
            current_WTS = i[3]
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
            print("Read R-TS: ",i[2])
            print("Read W-TS: ",i[3])
            print("Version: ",i[4])
            print("Key: ",i[0])
            print("Value: ",i[1])
            print()