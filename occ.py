read_sets = []
write_sets = []

startTS = []
validationTS = []
FinishTS = []

ti = []
# StartTS(Ti)
# Read and Execution Phase

# Read File
filename = input("Input Transaction's File: ")
f = open(filename, "r")

arrTransactions = []

cnt = 0
for i in (f.read().splitlines()):
    arrTransactions.append([i, cnt+1])

    n = 0
    for j in range(1, i.find("(")):
        n = n * 10 + int(i[j])

    if (n not in ti):
        ti.append(n)

    cnt += 1

f.close()
ti.sort()
ti.pop(0)

# Membuat array untuk setiap transaksi
arrTi = []
for i in ti:
    # formatnya itu Ti, startTS, validationTS, finishTS, read_sets, write_sets, all_sets
    arrTi.append([i, 0, 0, 0, [], [], []])

cnt = 0
for i in arrTransactions:
    n = 0
    if (i[0][0] == "C"):
        for j in range(1, len(i[0])):
            n = n * 10 + int(i[0][j])
    else:
        for j in range(1, i[0].find("(")):
            n = n * 10 + int(i[0][j])

    k = 0
    for j in arrTi:
        if j[0] == n and j[1] == 0:
            arrTi[k][1] = cnt + 1
        if j[0] == n and arrTransactions[cnt][0][0] == "R":
            arrTi[k][4].append(i)
        if j[0] == n and arrTransactions[cnt][0][0] == "W":
            arrTi[k][5].append(i)
        if j[0] == n:
            arrTi[k][6].append(i)
        k += 1
    cnt += 1

for i in arrTi:
    print("Transaksi: T"+str(i[0]))
    print(f"- startTS(T{i[0]}) = {i[1]}")
    print(f"- validationTS(T{i[0]}) = {i[2]}")
    print(f"- finishTS(T{i[0]}) = {i[3]}")
    if (len(i[4]) > 0):
        print(f"- read set untuk T{i[0]}:")
        for j in i[4]:
            print(j)
    if (len(i[5]) > 0):
        print(f"- write set untuk T{i[0]}:")
        for j in i[5]:
            print(j)
    if (len(i[6]) > 0):
        print(f"- seluruh transaksi pada T{i[0]}:")
        for j in i[6]:
            print(j)
    print("==========")

# ValidationTS(Ti)
# Validation Phase

# Write Phase
# FinishTS(Ti)
