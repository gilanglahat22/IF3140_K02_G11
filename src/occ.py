from transaction_occ import Transaction, build_action
import sys
import os
# StartTS(Ti)
# Read and Execution Phase

# Read File
# filename = input("Input Transaction's File: ")
f = open(sys.argv[1], "r")

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

trans_list = list(dictTrans.keys())
clock = 1


def is_all_finished(trans_list, dictTrans):
    for i in trans_list:
        if not(dictTrans[i].is_end):
            return False
    return True


while not is_all_finished(trans_list, dictTrans):
    for j in trans_list:
        if dictTrans[j].is_end:
            continue

        action = dictTrans[j].get_current_action()
        if (action.time <= clock):
            is_executed = True
            if (action.operation == "commit"):
                for i in trans_list:
                    if dictTrans[i].is_end:
                        if (not (dictTrans[i].finishTS < dictTrans[j].startTS) and
                            not((dictTrans[j].startTS < dictTrans[i].finishTS < clock) and
                                (len(dictTrans[i].write_sets.intersection(dictTrans[j].read_sets)) == 0))):
                            is_executed = False

            if is_executed:
                if action.operation == "commit":
                    print(f"<< validation T{j} = success >>")

                action.print(j)
                dictTrans[j].increment_state()

                if action.operation == "commit" or action.operation == "abort":
                    dictTrans[j].is_end = True
                    dictTrans[j].finishTS = clock
            else:
                print(f"<< validation T{j} = failed >>")
                print(f"A{j};")
                dictTrans[j].state = 0
                dictTrans[j].is_end = False
                dictTrans[j].startTS = clock

    clock += 1
