from transaction import Transaction, build_action

# StartTS(Ti)
# Read and Execution Phase

# Read File
filename = input("Input Transaction's File: ")
f = open(filename, "r")

arrTransactions = []
dictTrans: dict[int, Transaction] = dict()

cnt = 0
for i in (f.read().splitlines()):
    # arrTransactions.append([i, cnt+1])

    n = int(i[1:].split("(")[0])

    if (n not in dictTrans):
        tr = Transaction(n, cnt+1)
        tr.add_action(build_action(i, cnt+1))
        dictTrans[n] = tr
    else:
        dictTrans[n].add_action(build_action(i, cnt+1))

    cnt += 1

f.close()

for i in dictTrans:
    print("T"+str(dictTrans[i].number), dictTrans[i].actions)
    print("")
