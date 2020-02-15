import DBLoader as dbl
import QueryLauncher as ql

dbl.LoadCellcfgFiles(r'E:\data\cellcfg\20200203')
print("Launching the verification queries...")
oraconn = dbl.createOracleConnection()
ql.launchTheQueries(oraconn);


print("All done.")
