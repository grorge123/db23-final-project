def change(str, get):
    with open("./bench/src/main/resources/org/vanilladb/bench/vanillabench.properties", 'r') as file:
        lines = file.readlines()
        #go throught lines to find org.vanilladb.bench.benchmarks.ann.AnnBenchConstants.NUM_ITEMS...
        for i in range(len(lines)):
            if lines[i].find(str) != -1:
                lines[i] = "org.vanilladb.bench.benchmarks.ann.AnnBenchConstants." + str + "=" + get + "\n"
                break

    #write back to file
    with open("./bench/src/main/resources/org/vanilladb/bench/vanillabench.properties", 'w') as file:
        file.writelines(lines)

while(1):
    get = input("Change properties in vanillabench.properties ex: NUM_ITEMS 10000, or -1 to exit\n")
    if get == "-1":
        break
    change(get.split()[0], get.split()[1])