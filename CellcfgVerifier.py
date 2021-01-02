import DBLoader as dbl
import QueryLauncher as ql
import yaml as yml

config = yml.safe_load(open("CellcfgVerifier.yml"))
readDirectory = config['data']['directory']

dbl.LoadCellcfgFiles(readDirectory)
print("Launching the verification queries...")
oraconn = dbl.createOracleConnection()
ql.launchTheQueries(oraconn);


print("All done.")
