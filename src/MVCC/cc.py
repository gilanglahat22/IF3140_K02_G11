from storage import Storage
from mvcc import MVCC

class CC:

    ''' Transaction tabel with timestamp '''
    trTbl = {}

    ''' List contain transaction id which has been aborted '''
    trIDAborted = []

    ''' Queue to store transaction instruction'''
    inQueue = []

    ''' Queue contain transaction id which has been aborted  '''
    inQueueAborted = []

    ''' Object storage '''
    ObjectStorage = None

    ''' MVCC processor '''
    mvcc_processor = None

    def __init__(self, file : str, countItem : int):
        self.mvcc_processor = MVCC()
        self.inQueue = open(file, "r").read().split(" ")
        self.ObjectStorage = Storage(countItem)

    def execute(self):
        tsCount = 0

        while (len(self.inQueue) != 0 or len(self.inQueueAborted) != 0):
            # Initiate transaction which has been aborted
            if (self.inQueue == []):
                self.inQueue = self.inQueueAborted.copy()
                self.trTbl.clear()
                self.inQueueAborted.clear()
                self.trIDAborted.clear()
            
            # Processing intruction
            currIn = self.inQueue.pop()
            
            # Variabel to store key (object)
            key = None

            # Variabel to store information of key (object)
            value = None

            # Instruction type
            method = ""

            # Transaction ID
            trID = None

            # Transaction Timestamp
            trTS = None

            # Checking instruction type
            if(currIn.find("w") != -1): # Write
                method = "w"                
                currIn_split = currIn.split(method)
                trID = int(currIn_split[0])
                kvPair = currIn_split[1].split("|")

                key = int(kvPair[0])
                value = int(kvPair[1])

            elif(currIn.find("r") != -1): # Read
                method = "r"
                currIn_split = currIn.split(method)
                trID = int(currIn_split[0])
                key = int(currIn_split[1])

            else:
                print("Error detected at "+str(currIn)+". Please use r or w!")
                break
            
            if (len(currIn_split) < 2):
                print("Error detected at "+str(currIn)+". Please use correct format on file test.txt (Example: 1r0 or 2r1,100).")
                break
            
            if(trID not in self.trTbl.keys()): # If transaction ID not in transaction table
                trTS = tsCount # Assign trTS value with tsCount
                tsCount += 1
                self.trTbl[trID] = trTS
            
            else:
                # Otherwise, Assign trTS value with transaction table value from trID
                trTS = self.trTbl[trID]
            
            if(trID not in self.trIDAborted): # If transaction hasn't aborted
                # Execute read
                if (method == "r"):
                    result = self.mvcc_processor.read(key, value, self.ObjectStorage, trTS)

                # Execute write
                if (method == "w"):
                    result = self.mvcc_processor.write(key, value, self.ObjectStorage, trTS)
                
                # If result status is false, transaction will be aborting
                if (not result):
                    self.trIDAborted.append(trID)
                
                # Add instruction to Aborted queue
                if (trID in self.trIDAborted):
                    self.inQueueAborted.append(currIn)
            
            else: # Otherwise
                # Move to inQueueAborted, if it has been aborted
                self.inQueueAborted.append(currIn)

            print("----------------------------")
            print("Curr Instruction: ", currIn)
            print("Instruction Queue: ", self.inQueue)
            print("Transaction Table: ", self.trTbl)
            print("Aborted Transactions ID: ", self.trIDAborted)
            print("Aborted instructions Queue: ", self.inQueueAborted)
            print("Storage: \n")
            self.ObjectStorage.showStorage()