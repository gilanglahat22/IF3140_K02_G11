class SimpleLocking:
  def __init__(self):
    # List of transactions
    self.transactionList = []
    # List of locked transactions
    """
    Example
    lockDict = {
      A: 1
      B: 2
      ...
    }
    """
    self.lockDict = {}
    # Queue untuk menyimpan transaction yang harus waiting
    self.queue = []
    # List untuk menyimpan transaction yang sudah diproses
    self.done = []
    # List untuk menyimpan transactions yang terkenan pengaruh dari rollback
    self.rollback = []

  def concurrency_control(self):
    # Algoritma utama untuk simple locking concurrency control protocol
    while len(self.transactionList) > 0:
      curr = self.transactionList.pop(0)
      # Jika transaction dapat di-lock
      if self.can_lock(curr):
          self.lock(curr)
          self.info(curr)
          self.done.append(curr)
          if self.is_commit(curr):
            print(f"C{str(curr[1])} : Commit T{str(curr[1])}")
            self.unlock(curr)
            self.check_queue(curr)
      # Jika transaction tidak dapat di-lock
      else:
        print(f"Grant Lock for {self.format(curr)} denied.")
        if self.still_exist(curr):
          self.queue.append(curr)
          print(f"Transaction {self.format(curr)} added to queue.")
        else:
          print(f"Abort T{str(curr[1])}. Rollback T{str(curr[1])}.")
          self.execute_rollback(curr)
    return


  def read_from_file(self, filename):
    f = open("test/" + filename, "r")
    response = f.read()
    self.read(str(response))


  def read(self, input):
    # Proses membaca dan mengubah format transaction untuk diproses
    """
    Format Transaction
      [0] = R/W/C
      [1] = T ke n (1,2,3....,n)
      [2] = transaction data (A,B,C,dst)
    """
    transactions = input.split(',')
    for transaction in transactions:
      temp = []
      transaction = transaction.replace('(', ' ')
      transaction = transaction.replace(')', '')
      transaction = transaction.split(' ')
      if transaction[0][0] != 'C':
        temp = [transaction[0][0], int(transaction[0][1]), transaction[1]]
      else:
        temp = [transaction[0][0], int(transaction[0][1]), '']
      self.transactionList.append(temp)
    return

  def format(self, transaction):
    return f"{transaction[0]}{str(transaction[1])}({transaction[2]})"

  def output(self):
    res = []
    for transaction in self.transactionList:
      res.append(self.format(transaction))
    
    string = ", ".join(res)
    return string
    
  # Memeriksa apakah transaksi dapat di-lock
  def can_lock(self, transaction):
    # Jika sudah ada di lockDict, maka transaction tidak dapat di-lock 
    # dan harus menunggu sampai lock sebelumnya sudah di-unlock
    if transaction[2] in self.lockDict:
      # Jika transaksi sudah di-lock tapi memiliki nomor transaksi yang berbeda
      # maka transaksi saat ini tidak dapat di-lock
      if self.lockDict[transaction[2]] != transaction[1]:
        return False
    
    return True
  
  # Proses melakukan lock
  def lock(self, transaction):
    # Jika sudah dilock, tidak perlu di-lock lagi
    if transaction[2] in self.lockDict:
      if self.lockDict[transaction[2]] == transaction[1]:
        return

    # Jika belum, maka lakukan grant lock dengan memasukkan informasi transaksi ke lockDict    
    print(f"L{str(transaction[1])}({transaction[2]}) : Grant Lock-X({transaction[2]}) to T{str(transaction[1])}")
    self.lockDict[transaction[2]] = transaction[1]

  # Proses melakukan unlock
  def unlock(self, transaction):
    keyList = []
    for key, value in self.lockDict.items():
      if value == transaction[1]:
        keyList.append(key)
    for item in keyList:
      self.lockDict.pop(item)
      print(f"UL{str(transaction[1])}({item}): Unlock Lock-X({str(transaction[1])}) from T{transaction[1]}")
  
  # Memeriksa apakah ada transaksi yang harus di-rollback
  def check_rollback(self, transaction):
    for element in self.done:
      if element[1] == transaction[1]:
        self.rollback.append(element)
        self.done.remove(element)
        print(f"Rollback {element[0]}{str(element[1])}({element[2]})")

  # Memeriksa apakah masih terdapat transaksi yang belum diproses    
  def check_remaining(self, transaction):
    for element in self.transactionList:
      if element[1] == transaction[1]:
        self.rollback.append(element)
        self.transactionList.remove(element)

  # Memeriksa apakah masih terdapat transaksi yang menunggu
  def check_queue(self, transaction):
    value = None
    for element in self.queue:
      if element[2] == transaction[2]:
        value = element
        break
    # Jika terdapat transaksi yang menunggu, maka lakukan lock dan unlock transaksi
    if value is not None:
      print(f"L{str(value[1])}({value[2]}): Grant Lock-X({value[2]}) to T{str(value[1])}")
      print("From Queue: ", end="")
      self.info(value)
      print(f"UL{str(value[1])}({value[2]}): Unlock Lock-X({value[2]}) from T{str(value[1])}")
      print(f"C{str(value[1])} : Commit T{str(value[1])}")
      self.queue.remove(value)

  def is_commit(self, transaction):
    for element in self.transactionList:
      if element[1] == transaction[1]:
        return False
    return True

  # Proses menjalankan rollback
  def execute_rollback(self, transaction):
    self.check_rollback(transaction)
    self.rollback.append(transaction)
    self.check_remaining(transaction)
    self.unlock(transaction)
    self.transactionList += self.rollback
    self.rollback = []

  # Output informasi
  def info(self, transaction):
    if transaction[0] == 'R':
      print(f"{self.format(transaction)} : T{str(transaction[1])} reads {transaction[2]}")
    else:
      print(f"{self.format(transaction)} : T{str(transaction[1])} writes {transaction[2]}")

  # Memeriksa apakah nomor transaksi masih terdapat pada transaksi berikutnya
  def still_exist(self, transaction):
    for element in self.transactionList:
      if element[1] == transaction[1]:
        return False
    for element in self.done:
      if element[1] == transaction[1]:
        return False
    return True

if __name__ == "__main__":
  simple_locking = SimpleLocking()
  print("\nSimple Locking Concurrency Control Protocol")
  print("Contoh Transasctions:", end=" ")
  print("R1(A),R2(B),W1(A),R1(B),W3(A),W4(B),W2(B),R1(C)", end="\n\n")
  input_type = input("Input from file or keyboard? ")
  if input_type == 'file':
    simple_locking.read_from_file(input("Enter file name (*.txt): "))
  else:
    simple_locking.read(input("Enter transactions: "))
  print("\nSimple Locking Concurrency Control Result:")
  simple_locking.concurrency_control()