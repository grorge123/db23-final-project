def change_bench(str, get):
    path = "./bench/src/main/resources/org/vanilladb/bench/vanillabench.properties"
    with open(path, 'r') as file:
        lines = file.readlines()
        for i in range(len(lines)):
            if lines[i].find(str) != -1:
                lines[i] = lines[i].split('=')[0] + "=" + get + "\n"
                break
    #write back to file
    with open(path, 'w') as file:
        file.writelines(lines)

def change_core(str, get):
    path = "./bench/src/main/resources/org/vanilladb/core/vanilladb.properties"
    with open(path, 'r') as file:
        lines = file.readlines()
        for i in range(len(lines)):
            if lines[i].find(str) != -1:
                lines[i] = lines[i].split('=')[0] + "=" + get + "\n"
                break
    #write back to file
    with open(path, 'w') as file:
        file.writelines(lines)

def change(str, second):
    print("change " + str + " (-1 to remain the same):")
    num_items = input()
    if num_items != "-1":
        if second == 1:
            change_bench(str, num_items)
        change_core(str, num_items)

change("NUM_ITEMS", 1)
change("NUM_DIMENSIONS", 1)
change("NUM_NEIGHBORS", 0)
change("NUM_GROUPS", 0)
