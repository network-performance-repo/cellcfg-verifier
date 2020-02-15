import DBLoader as dbl
import pandas as pd
import cx_Oracle

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

def fetchTheQueries(oraconn):

    cur = oraconn.cursor()
    oraconn.outputtypehandler = OutputTypeHandler

    try:
        cur.execute("select tech,vendor,target,query_ from global_gsm.cellcfg_query_repository")
    except cx_Oracle.DatabaseError as e:
        print("There is a problem in fetching the queries list.", e)

    queries = cur.fetchall()

    cur.close()

    return queries

def launchTheQeury(query, oraconn):
    tech,vendor,target,query_ = query
    print('Launching the query ', tech + '_' + vendor + '_' + target, ' ...')

    df_ora = pd.read_sql(query_, con=oraconn)

    print('Finished Launching the query  ', tech + '_' + vendor + '_' + target)

    return df_ora

def saveTheResult(query, df_result, writer):
    tech,vendor,target,query_ = query
    df_result.to_excel(writer, sheet_name=tech + '_' + vendor + '_' + target)

def appendTheSummary(query, df_result, arr_result_summary_data):
    issuedRows = len(df_result.index)
    tech, vendor, target, query_ = query
    testCaption = 'Rows with issue in ' + tech + '_' + vendor + '_' + target
    arr_result_summary_data.append([testCaption, issuedRows])

def launchTheQueries(oraconn):
    queries = fetchTheQueries(oraconn)

    writer = pd.ExcelWriter(r'E:\data\cellcfg\20200128\cellcfg_report_20200128.xlsx')

    arr_result_summary_data = [] # this is to collect summary of test result in a loop and later to be written to
        # first excel sheet
    df_result_summary = pd.DataFrame()
    df_result_summary.to_excel(writer, sheet_name='Test Result Summary')# have to create this sheet first as
        # I cannot move the sheet to first later. at least I don't know how yet.

    progressCounter = 0
    totalCount = len(queries)

    for query in queries:
        df_result = launchTheQeury(query, oraconn)
        saveTheResult(query, df_result, writer)

        progressCounter += 1
        print(progressCounter, ' out of ' , totalCount, ' finished.')

        appendTheSummary(query, df_result, arr_result_summary_data)


    df_result_summary = pd.DataFrame(data=arr_result_summary_data, columns=['Test', 'Result'])
    df_result_summary.to_excel(writer, sheet_name='Test Result Summary')

    writer.save()
