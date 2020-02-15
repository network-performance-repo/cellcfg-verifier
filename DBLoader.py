#import OracleConnection as dao
import cx_Oracle as cxora
import csv
import glob


def detectTableName(filename:str):
    tablename = ''
    columnList = ''
    placeholderList = ''

    if filename.__contains__('CellCFG-2G'):
        tablename = 'global_gsm.maps_cellcfg_2g_temp'
        columnList = 'TIME_,CELL_2G,BSC_2G,BTS_2G,PROVINCE,CITY,REGION,VENDOR,SITE_PRIORITY,CI,LAC,LATITUDE,LONGITUDE,DTCH,CLUSTER_REF,REGION_VENDOR,PRIORITY,FIRSTCOLLECTTIME ,LASTCOLLECTTIME ,ONAIR,MUNICIPALITY_REGION,ADDRESS1,ADDRESS2,FREQS900,FREQS1800,BAND1800,BAND900,TRX1800,TRX900,ANTENNA18,ANTENNA9,VERTICALBW18,VERTICALBW9,TILT18,TILT9,SITENAME,Rotate_angle,HEIGHT,location_number,Technology,Site_ID_CFG,SUB_NETWORK_CFG,NPO_REGION_CFG,AZIMUTH18,AZIMUTH9,HORIZONTALBW18,HORIZONTALBW9,TCH_AVAILABILITY_IR_2G'
        placeholderList = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44, :45, :46, :47, :48'
    elif filename.__contains__('CellCFG-3G'):
        tablename = 'global_gsm.maps_cellcfg_3g_temp'
        columnList = 'TIME_,WCELL_3G,NODEB_3G,RNC_3G,PROVINCE,CITY,REGION,VENDOR,REGION_VENDOR,LAC,CI,CLUSTER_REF,SITE_PRIORITY,LATITUDE,LONGITUDE,DL_UARFCN,PRMSCRAMBLING_CODE,RAC,SAC,UL_UARFCN,MAX_POWER,CPICH_POWER,URAID,Address1,Address2,ELECTRICAL_TILT,MUNICIPALITY_REGION,Site_Name,AZIMUTH,HORIZENTAL_BEAMWIDTH,MECHANICAL_TILT,SUB_NETWORK_CFG,VERTICAL_BEAMWIDTH,NPO_REGION_CFG,Site_ID_CFG,location_number,HEIGHT,firstCollectTime,lastCollectTime,Onair,Technology,PAYLOAD_TOTAL_3G_MBYTE_IR_3G,CELL_AVAIL_SYS_IR_3G'
        placeholderList = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43'
    elif filename.__contains__('CellCFG-4GFDD'):
        tablename = 'global_gsm.maps_cellcfg_4g_temp'
        columnList = 'TIME_4G,LTE_CELL,LTE_ENODB_4G,VENDOR,FIRSTCOLLECTTIME,LASTCOLLECTTIME,SITE,REGION,PROVINCE,CITY,TAC,CLUSTER_REF,CI,PCI,LATITUDE,LONGITUDE,ENODEBID,DL_EARFCN,UL_EARFCN,DL_BW,ECI,DL_Freq,UL_Freq,MUNICIPALITY_REGION,Site_Name,Site_Priority,Address1,Address2,ELECTRICAL_TILT,SUB_NETWORK_CFG,Site_ID_CFG,location_number,Technology,NPO_REGION_CFG,Onair,HEIGHT,VERTICAL_BEAMWIDTH,ne_name,ROTATE_ANGLE,PAYLOAD_TOTAL_MBYTE_IR_MB_4G,CELL_AVAIL_SYS_IR_4G'
        placeholderList = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41'
    elif filename.__contains__('CellCFG-4GTDD'):
        tablename = 'global_gsm.maps_cellcfg_tdd_temp'
        columnList = 'TIME,TDD_CELL_4G,TDD_ENODEB_4G,FIRSTCOLLECTTIME,LASTCOLLECTTIME,SITE,"On air",REGION,PROVINCE,CITY,CLUSTER_REF,CI,LATITUDE,LONGITUDE,TAC,PCI,ENODEBID,ELECTRICAL_TILT,SITE_PRIORITY,Vendor,ECI,UL_EARFCN,DL_EARFCN,DL_BW,MUNICIPALITY_REGION,Site_Name,Address1,Address2,NPO_REGION_CFG,SITE_ID_CFG,SUB_NETWORK_CFG,location_number,Technology,HEIGHT,ne_name,ROTATE_ANGLE,PAYLOAD_TOTAL_TB_IR_TDD,CELL_AVAIL_SYS_IR_4G_TDD'
        placeholderList = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38'

    return tablename, columnList, placeholderList

def insertChunk(cursor, chunkArray, databaseTableName:str, tableColumnList:str, placeholderList:str, oraConn):
    try:
        prepareStatement = 'insert into ' + databaseTableName + ' (' + tableColumnList \
                           + ') values (' + placeholderList + ')'

        cursor.prepare(prepareStatement)

        cursor.executemany(None, chunkArray) #None because the statement has already prepared

    except cxora.DatabaseError as e:
        print("Exception occurred when inserting: ", e)
        cursor.close()

    except Exception as e:

        print("Exception occurred when inserting: ", e)
        cursor.close()

    oraConn.commit()

def loadData(csv_reader, filename:str, oraConn):
    databaseTableName, tableColumnList, placeholderList = detectTableName(filename)

    cursor = oraConn.cursor()
    # truncate table first
    statement = 'truncate table ' + databaseTableName
    cursor.execute(statement)

    chunkArray = []
    rowCounter = 0
    line = next(csv_reader)  # skip 1st row
    line = next(csv_reader)  # skip 2nd row
    # lst_csv = df_csv.values.tolist()
    i = 0
    chunkArray = []
    for line in csv_reader:
        chunkArray.append(line)
        rowCounter += 1
        if rowCounter % 1000 == 0: #chuck size = 1000
            insertChunk(cursor, chunkArray, databaseTableName, tableColumnList, placeholderList, oraConn)
            print("Inserted rows ", rowCounter)
            chunkArray = [] #reset the chunk array

    #inserting the remaining rows
    insertChunk(cursor, chunkArray, databaseTableName, tableColumnList, placeholderList, oraConn)
    print("Inserted rows ", rowCounter)


def createOracleConnection():
    ip = '10.133.154.175'
    port = 1521
    SID = 'stat4p'

    dsn_tns = cxora.makedsn(ip, port, SID)

    try:
        connection = cxora.connect('GLOBAL_GSM', 'xc%$q2', dsn_tns)
        print("Connection Created successful")

    except cxora.DatabaseError as e:
        print("There is a problem with Oracle", e)

    return connection

def LoadCellcfgFiles(path:str):
    oraConn = createOracleConnection()

    print('Loading CELLCFG files in the path ' + path + ' ...')
    all_files = glob.glob(path + "/*CellCFG*.csv")

    print('Found these files in the path ', all_files)

    listCELLCFGFiles = []

    for filename in all_files:
        print('Loading ', filename , ' ...')
        #filename = "E:\workspace_python\cellcfg\CellCFG-4GTDD_2019093008_8f9a72399a134fc596d72897a618be67_081425.csv"
        # df_csv = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',',index_col=None, header=0)
        csv_reader = csv.reader(open(filename, "r"))
        loadData(csv_reader, filename, oraConn)

    print('All CELLCFG files were Loaded.')

    oraConn.close()



