# %% Libraries import
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
import psycopg2
import re



# %% SQL_Query


def db_query(sql_stmt):
    # %% Parameter
    ssh_connection = {
        'host': '52.57.67.19',
        'host_port': 22,
        'ssh_user': 'ec2-user',
        'ssh_private_key': r"C:\Users\JoshuaBraun\Documents\Zugangsdaten Main\test-key.pem", #path to your local key-file
        'remote_host': 'testpg.ctw8fyjalktb.eu-central-1.rds.amazonaws.com',
        'remote_port': 5432,
        'local_host': 'localhost',
        'local_port': 5432,
    }

    db_name = 'maintellisense'
    db_user = 'main'
    db_password = 'n1WkBb4LJdwf7AjAXwMY'

    with SSHTunnelForwarder(
            (ssh_connection["host"], ssh_connection["host_port"]),
            ssh_username=ssh_connection["ssh_user"],
            ssh_pkey=ssh_connection["ssh_private_key"],
            remote_bind_address=(
                    ssh_connection["remote_host"], ssh_connection["remote_port"]),
            local_bind_address=(ssh_connection["local_host"], ssh_connection["local_port"])) as ssh_tunnel:
        ssh_tunnel.start()
        url = f"postgresql://{db_user}:{db_password}@localhost:5432/{db_name}"
        sqlEngine = create_engine(url)
        dbConnection = sqlEngine.connect()
        df_sql_query = pd.read_sql(sql_stmt, dbConnection)
        dbConnection.close()
        ssh_tunnel.close()

    del sqlEngine, dbConnection
    return df_sql_query

def check_for_errors(string_list):
    
    error_regex_list = [re.compile(r'fehl(?!erfrei)', re.IGNORECASE), 
                        re.compile(r'not', re.IGNORECASE), 
                        re.compile(r'warn', re.IGNORECASE),
                        re.compile(r'stop', re.IGNORECASE),
                        re.compile(r'inaktiv', re.IGNORECASE),
                        re.compile(r'defekt', re.IGNORECASE),
                        re.compile(r'aus', re.IGNORECASE),
                        re.compile(r'nicht', re.IGNORECASE),
                        re.compile(r'halt', re.IGNORECASE),
                        re.compile(r'ueber', re.IGNORECASE),
                        re.compile(r'alarm', re.IGNORECASE),
                        re.compile(r'stör', re.IGNORECASE),
                        re.compile(r'stoer', re.IGNORECASE),
                        re.compile(r'abweich', re.IGNORECASE),
                        re.compile(r'verzögert', re.IGNORECASE),
                        re.compile(r'verzoegert', re.IGNORECASE),
                        re.compile(r'sicher-halt', re.IGNORECASE),
                        re.compile(r'defekt', re.IGNORECASE),
                        re.compile(r'gesperrt', re.IGNORECASE),
                        re.compile(r'außerhalb gültigem Bereich', re.IGNORECASE),
                        ]

# List to store strings matching the regular expressions
    matching_strings = []

# Iterate through the list of strings
    for s in string_list:
        # Check each regular expression in the list
        for error_regex in error_regex_list:
            if error_regex.search(s):
                # If a match is found, add the string to the matching_strings list
                matching_strings.append(s)
                break
            
    return matching_strings


# %% extract timestamps for Andruck und Fortdruckphase
def extract_production_run_frame(start_time, end_time, id=8961):
    '''
    Will extract the timestamps and the corresponding eventlog of a timeframe from the database
    '''
    ruest_sql_string_start= f"select eventtime,eventobject,eventnumber from log.event where deviceid = {str(id)} and eventtime >= '{start_time}' and eventtime <= '{end_time}';" 
    ruest_starts_frame = db_query(ruest_sql_string_start) #yields db-rows with starts of the Ruestphases in given timeframe
    return ruest_starts_frame

def extract_phase_stamps(event_frame):
    Andruck_start = []
    Andruck_ende_Fortdruck_start = []
    Fortdruck_ende = []
    Druck_an = []
    Druck_ab = []
    Letztes_Schneller = None
    Fortdruck_in_prog = False
    Netto_aus = False
    for index, event in enumerate(event_frame["eventobject"]):
        if event == 555 and event_frame["eventnumber"].iloc[index] == 3:
            Letztes_Schneller = index
        elif event == 322 and event_frame["eventnumber"].iloc[index] == 1:
            try:
                Andruck_start.append(event_frame["eventtime"].iloc[Letztes_Schneller]) 
            except:
                IndexError
            if Letztes_Schneller is not None:
                Andruck_ende_Fortdruck_start.append(event_frame["eventtime"].iloc[index])
                Fortdruck_in_prog = True
        elif event == 322 and event_frame["eventnumber"].iloc[index] == 2 and Fortdruck_in_prog == True:
            Netto_aus = True
        elif event == 480 and event_frame["eventnumber"].iloc[index] == 1 and Fortdruck_in_prog == True and Netto_aus == True:
            Fortdruck_ende.append(event_frame["eventtime"].iloc[index])
            Fortdruck_in_prog = False
            Netto_aus = False
        elif event == 511 and event_frame["eventnumber"].iloc[index] == 1:
            Druck_an.append(event_frame["eventtime"].iloc[index])
        elif event == 511 and event_frame["eventnumber"].iloc[index] == 0:
            Druck_ab.append(event_frame["eventtime"].iloc[index])
    
    return Andruck_start, Andruck_ende_Fortdruck_start, Fortdruck_ende, Druck_an, Druck_ab

def phase_frame_merger(Andruck_start, Andruck_zu_Fortdruck, Fortdruck_ende, sensor_data_frame):
    Andruck_frame = pd.DataFrame()
    Fortdruck_frame = pd.DataFrame()
    
    index_an = 1
    index_fort = 1
    for i in range(len(Andruck_start)):
        
        '''
        create dataframes consisting only of a single type of these two productionphases. As there is always the same number
        of productionphases, passing one vector of phases is enough 
        '''
        # creating the Andruck-Phase frame
        new_appendage_a = sensor_data_frame[(sensor_data_frame["_time"] >= Andruck_start[i]) & (sensor_data_frame["_time"] < Andruck_zu_Fortdruck[i])]
        new_appendage_a["_production_run_index"] = [index_an for _ in range(len(new_appendage_a))]
        new_appendage_a["_x_time"] = [_ for _ in range(len(new_appendage_a))]
        if len(new_appendage_a) < 900:
            Andruck_frame = pd.concat([Andruck_frame, new_appendage_a], axis=0)
            index_an += 1
        else:
            pass
        # creating the Fortdruck-Phase frame
        new_appendage_f = sensor_data_frame[(sensor_data_frame["_time"] >= Andruck_zu_Fortdruck[i]) & (sensor_data_frame["_time"] <= Fortdruck_ende[i])]
        new_appendage_f["_production_run_index"] = [index_fort for __ in range(len(new_appendage_f))]
        new_appendage_f["_x_time"] = [__ for __ in range(len(new_appendage_f))]
        if len(new_appendage_f) < 20000:
            Fortdruck_frame = pd.concat([Fortdruck_frame, new_appendage_f], axis=0)
            index_fort += 1
        else:
            pass
    return Andruck_frame, Fortdruck_frame

def extract_event_descriptions_for_timewindows(start, end, id=8961):
    '''
    Construct a numbered dataframe containing all events occuring within a number of timeframes defined by two lists 
    '''
    assert len(start)==len(end), "Pass lists of equal length"
    eventgroups =[30,31,5,6,9,1,23,24]
    FP_counter = 0
    TP_counter = 0
    event_frame = pd.DataFrame()
    query_string = f"SELECT e.eventtime, e.eventobject, e.eventnumber, e.eventgroup, g.eventtext FROM log.event AS e JOIN log.germaneventtexts AS g ON e.eventobject = g.obj AND e.eventnumber::text = g.num WHERE e.deviceid = {str(id)} AND e.eventtime >= '{start[0]}' AND e.eventtime <= '{end[len(end)-1]}';"
    entire_frame = db_query(query_string).sort_values(by = "eventtime")
    entire_frame["eventtime"] = pd.to_datetime(entire_frame["eventtime"].values)
    
    for i in range(len(start)):
        new_frame= entire_frame.loc[(entire_frame["eventtime"]>=pd.to_datetime(start[i]).to_numpy()) & (entire_frame["eventtime"]<=pd.to_datetime(end[i]).to_numpy())]
        if len(new_frame) == 0:
            FP_counter+=1
            continue
        else:
            if not new_frame["eventgroup"].isin(eventgroups).any():
                FP_counter+=1
                continue
            elif ((new_frame["eventobject"] != 5769) & (new_frame["eventnumber"] != 1)).any():  #check whether any event is not the successful finish
                TP_counter+=1
        new_frame = new_frame.drop_duplicates(subset=["eventobject","eventnumber"])
        new_frame["_time_frame_index"] = [i+1 for _ in range(len(new_frame))]
        new_frame["_event_position"] = [len(new_frame) - position for position in range(len(new_frame))]
        event_frame = pd.concat([event_frame, new_frame], axis=0)
    
    return event_frame, TP_counter,FP_counter
        
        
if __name__ == "__main__":
    end = '2023-10-30 14:50:00.000+0100' 
    start = '2023-10-24 09:11:48.000+0100'
    frame = extract_production_run_frame(start,end)
    print(frame)
    

''''
    
    #%% Liste mit allen Devices
    sql_string = "select press, pressid, count(incidenttype) as webbreak_counts from cloud.zpm_paperincidents \
        where incidenttype = 'webbreak' group by press,pressid order by webbreak_counts desc;"
    df_web_devices = db_query(sql_string)

    #%% Liste mit allen Webbreaks der Devices
    deviceIds_wb = df_web_devices['pressid']
    all_wb = pd.DataFrame()
    for x in deviceIds_wb:
        device_ID = x
        sql_string = f"select runid, pressid, incidenttime, firstdetectingunit from cloud.zpm_paperincidents where incidenttype = 'webbreak'  and pressid = {device_ID} order by incidenttime desc;"
        temp_wb = db_query(sql_string)
        all_wb=all_wb.append(temp_wb)

    all_wb.to_csv('all_wb_2308.csv', encoding='utf-8', header='true')

    all_wb = pd.read_csv('all_wb_2308.csv', quotechar='"', sep=',')
    #%% Subset nach Ort
    all_wb['units'] = all_wb['firstdetectingunit'].str.slice(0, 3)

    table = all_wb['units'].value_counts()
    print(table / table.sum())

    incidents_PU = all_wb[all_wb['units'] == "PU "] # check
    incidents_WCD = all_wb[all_wb['units'] == "WCD"] # check
    incidents_FSS = all_wb[all_wb['units'] == "FSS"] # check
    incidents_CU = all_wb[all_wb['units'] == "CU "] # check
    incidents_FU = all_wb[all_wb['units'] == "FU "] # check
    incidents_RS = all_wb[all_wb['units'] == "RS "] # Reel Splicer - Rollenwechsler



    # incidents_other = all_wb[all_wb['units'] !=["PU ", "WCD", "FSS", "CU ", "FU ", "RS "]]

    all_wb['incident_focus'] = 1 if all_wb['units'] = ["PU ", "WCD", "FSS", "CU ", "FU ", "RS "] else 0

    all_wb['incident_focus'] = 0

    all_wb.loc[all_wb['units'].str.contains('PU '), 'incident_focus'] = 1
    all_wb.loc[all_wb['units'].str.contains('WCD'), 'incident_focus'] = 1
    all_wb.loc[all_wb['units'].str.contains('FSS'), 'incident_focus'] = 1
    all_wb.loc[all_wb['units'].str.contains('CU '), 'incident_focus'] = 1
    all_wb.loc[all_wb['units'].str.contains('FU '), 'incident_focus'] = 1
    all_wb.loc[all_wb['units'].str.contains('RS '), 'incident_focus'] = 1

    all_wb['incident_focus'].value_counts()

    incidents_other = all_wb[all_wb['incident_focus'] == 0]


    #%% Events other

    temp_event_df = pd.DataFrame()
    full_event_df = pd.DataFrame()

    for pressid, incidenttime in zip(incidents_other.pressid, incidents_other.incidenttime):
        webbreak_time = pd.to_datetime(incidenttime)
        timestamp_before_incident = webbreak_time - pd.Timedelta(2, unit="m")
        sql_string = f"select id, runid, params, eventtime, eventobject, eventnumber from log.event where eventtime >= '{timestamp_before_incident}' and eventtime <= '{webbreak_time}' and deviceid={pressid} order by eventtime desc;"
        temp_event_df = db_query(sql_string)
        full_event_df=full_event_df.append(temp_event_df)

    full_event_df['Location'] = "other"

    full_event_df.to_csv('wb_events_other.csv', encoding='utf-8', header='true')



    #%% Zusammenfügen der Datensätze und Durchführen der Berechnungen
    # Merge datasets

    wb_events_PU = pd.read_csv('wb_events_PU.csv', quotechar='"', sep=',')
    wb_events_WCD = pd.read_csv('wb_events_WCD.csv', quotechar='"', sep=',')
    wb_events_FSS = pd.read_csv('wb_events_FSS.csv', quotechar='"', sep=',')
    wb_events_CU = pd.read_csv('wb_events_CU.csv', quotechar='"', sep=',')
    wb_events_FU = pd.read_csv('wb_events_FU.csv', quotechar='"', sep=',')
    wb_events_RS = pd.read_csv('wb_events_RS.csv', quotechar='"', sep=',')
    wb_events_other = pd.read_csv('wb_events_other.csv', quotechar='"', sep=',')

    frames = [wb_events_PU, wb_events_WCD, wb_events_FSS, wb_events_CU, wb_events_FU, wb_events_RS, wb_events_other]
    full_event_wb = pd.concat(frames)

    full_event_wb.to_csv('full_event_wb.csv', encoding='utf-8', header='true')

    #%% Concat germaneventtext-column
    event_texts = pd.read_excel('event_texts.xlsx')

    event_texts.rename(columns = {'obj':'eventobject', 'num':'eventnumber'}, inplace = True)
    event_texts = event_texts[event_texts["eventnumber"].str.contains("None") == False]

    full_event_wb = pd.merge(full_event_wb, event_texts, on=["eventobject", "eventnumber"], how="left")

    #%%
    # Analyze Frequencies



    wb_events_PU["count"] = wb_events_PU.value_counts(subset=['eventobject', 'eventnumber'])
    PU_counts.to_csv('PU_counts.csv', , encoding='utf-8', header='true')

    event_freqs = pd.DataFrame()
    event_freqs = wb_events_PU.value_counts(subset=['eventobject', 'eventnumber'])


    pd.crosstab(index=full_event_wb["eventobject", "eventnumber"], columns="count")

    df.groupby(['make', 'body_style'])['body_style'].count().unstack().fillna(0)


    PU_count = wb_events_PU.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_PU")
    WCD_count = wb_events_WCD.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_WCD")
    FSS_count = wb_events_FSS.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_FSS")
    CU_count = wb_events_CU.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_CU")
    FU_count = wb_events_FU.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_FU")
    RS_count = wb_events_RS.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_RS")
    other_count = wb_events_other.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_other")
    full_count = full_event_wb.groupby(["eventnumber", "eventobject"]).size().reset_index(name="count_all")


    PU_count['PU_prob'] = PU_count["count_PU"] / len(incidents_PU)
    WCD_count['WCD_prob'] = WCD_count["count_WCD"] / len(incidents_WCD)
    FSS_count['FSS_prob'] = FSS_count["count_FSS"] / len(incidents_FSS)
    CU_count['CU_prob'] = CU_count["count_CU"] / len(incidents_CU)
    FU_count['FU_prob'] = FU_count["count_FU"] / len(incidents_FU)
    RS_count['RS_prob'] = RS_count["count_RS"] / len(incidents_RS)
    other_count['other_prob'] = other_count["count_other"] / len(incidents_other)
    full_count['all_prob'] = full_count["count_all"] / len(all_wb)

    full_count = pd.merge(full_count, event_texts, on=["eventobject", "eventnumber"], how="left")

    full_count = pd.merge(full_count, PU_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, WCD_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, FSS_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, CU_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, FU_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, RS_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, other_count, on=["eventobject", "eventnumber"], how="left")
    full_count = pd.merge(full_count, event_texts, on=["eventobject", "eventnumber"], how="left")

    full_count.to_excel('full_count.xlsx')
    '''