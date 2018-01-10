import pandas as pd
import os, re
from pprint import pprint

base_folder = "/Users/ocalvet/Documents/MS BGD/INF728 - BDD NoSQL/Projet"

def get_filenames_for_day(day, data_type):

    with open(os.path.join(base_folder, "ls_telecom.gdelt")) as my_file:
        return re.findall(r"({}\d+\.{}\.{}\.zip)".format(day,
                                                         data_type.split(".")[0],
                                                         data_type.split(".")[1] )
                          , my_file.read())

def download_files(path_to_full_CSVs, day, data_type):

    path_to_full_CSV = os.path.join(path_to_full_CSVs, day + "_{}_full.csv".format(data_type.split(".")[0]))

    if os.path.exists(path_to_full_CSV):
        print("Downloads skipped => {} already existed".format(path_to_full_CSV))
    else :
        # One day data in one shot (really long response from s3 so I don't use that but I keep
        # the command as a reminder since we can load all files we need in a single command
        # using proper includes
        """
        os.system('aws s3 cp s3://telecom.gdelt/ {}/ --recursive --exclude "*" --include "{}*.{}.zip'.\
                  format(path_to_full_CSVs.replace(" ", "\ "), day, data_type))
        """

        filenames = get_filenames_for_day(day=day, data_type=data_type)
        nb_files = len(filenames)
        print("nb files for day", day, nb_files)

        for i, filename in enumerate(filenames):
            os.system("aws s3 cp s3://telecom.gdelt/{} {}/".format(filename, path_to_full_CSVs.replace(" ", "\ ")))
            print("Download {}/{} : {} done".format(i + 1, nb_files, filename))

        # Unzip all files for a day
        os.system('unzip "{}/*.zip" -d "{}"'.format(path_to_full_CSVs, path_to_full_CSVs))
        # Then concatenate them in one big file
        os.system('cat {}/{}*.{} > {}'.format(path_to_full_CSVs.replace(" ", "\ "), day, data_type, path_to_full_CSV.replace(" ", "\ ")))
        # remove the initial multiple small files
        os.system('rm {}/{}*.{}*'.format(path_to_full_CSVs.replace(" ", "\ "), day, data_type))

    return path_to_full_CSV



colnames = "GLOBALEVENTID;SQLDATE;MonthYear;Year;FractionDate;Actor1Code;Actor1Name;Actor1CountryCode;Actor1KnownGroupCode;Actor1EthnicCode;Actor1Religion1Code;Actor1Religion2Code;Actor1Type1Code;Actor1Type2Code;Actor1Type3Code;Actor2Code;Actor2Name;Actor2CountryCode;Actor2KnownGroupCode;Actor2EthnicCode;Actor2Religion1Code;Actor2Religion2Code;Actor2Type1Code;Actor2Type2Code;Actor2Type3Code;IsRootEvent;EventCode;EventBaseCode;EventRootCode;QuadClass;GoldsteinScale;NumMentions;NumSources;NumArticles;AvgTone;Actor1Geo_Type;Actor1Geo_FullName;Actor1Geo_CountryCode;Actor1Geo_ADM1Code;Actor1Geo_ADM2Code;Actor1Geo_Lat;Actor1Geo_Long;Actor1Geo_FeatureID;Actor2Geo_Type;Actor2Geo_FullName;Actor2Geo_CountryCode;Actor2Geo_ADM1Code;Actor2Geo_ADM2Code;Actor2Geo_Lat;Actor2Geo_Long;Actor2Geo_FeatureID;ActionGeo_Type;ActionGeo_FullName;ActionGeo_CountryCode;ActionGeo_ADM1Code;ActionGeo_ADM2Code;ActionGeo_Lat;ActionGeo_Long;ActionGeo_FeatureID;DATEADDED;SOURCEURL".split(
    ";")

cols_to_keep = ["GLOBALEVENTID",
                "SQLDATE",
                "MonthYear",
                "EventRootCode",
                "NumMentions",
                "AvgTone",
                "ActionGeo_CountryCode",
                "SOURCEURL"]

def prepare_ligther_events_file(path_to_full_CSV, path_to_lighter_events):

    filename = os.path.split(path_to_full_CSV)[-1]
    lighterfilepath = os.path.join(path_to_lighter_events, filename)

    if not os.path.exists(path_to_full_CSV):
        print("File {} skipped => {} doesn't existed".format(filename, path_to_full_CSV))

    else:
        if not os.path.exists(lighterfilepath):

            df = pd.read_csv(path_to_full_CSV, sep="\t", names=colnames, low_memory=False)
            # Writing new csv with less columns
            df.loc[:, cols_to_keep].to_csv(lighterfilepath, sep=";", index=False)
            print("Lighter events csv done for {}".format(filename))
        else :
            print("Lighter events csv {} skipped => {} already existed".format(filename, lighterfilepath))

def print_geotype(path_to_full_CSV):

    filename = os.path.split(path_to_full_CSV)[-1]

    if not os.path.exists(path_to_full_CSV):
        print("File {} skipped => {} doesn't existed".format(filename, path_to_full_CSV))

    else:
        df = pd.read_csv(path_to_full_CSV, sep="\t", names=colnames, low_memory=False)
        print(df.dtypes)
        nb_geo_1 = len(df[df["ActionGeo_Type"] == 1]) / len(df) * 100
        print("nb geo 1 : {}%".format(nb_geo_1))
        nb_missing = len(df[df["ActionGeo_CountryCode"].isnull()]) / len(df) * 100
        print("nb_missing : {}%".format(nb_missing))

def prepare_cql_commands(filename_pattern):

    txt = """CREATE TABLE november.events
(	
GLOBALEVENTID int, 
SQLDATE int, 
MonthYear int,
EventRootCode int,
NumMentions int,
AvgTone TEXT, 
ActionGeo_CountryCode TEXT,
SOURCEURL TEXT,

PRIMARY KEY ((SQLDATE, EventRootCode), ActionGeo_CountryCode, GLOBALEVENTID)
) WITH CLUSTERING ORDER BY (ActionGeo_CountryCode ASC);

COPY november.events
(GLOBALEVENTID, 
SQLDATE, 
MonthYear,
EventRootCode, 
NumMentions,
AvgTone,
ActionGeo_CountryCode,
SOURCEURL)
FROM '{}'
WITH HEADER=TRUE and DELIMITER=';';
""".format(filename_pattern)

    return txt

if __name__ == "__main__" :

    yearmonth = "201711"

    path_to_events = os.path.join(base_folder, "events")
    path_to_lighter_events = os.path.join(base_folder, "lighter_events")

    if not os.path.exists(path_to_events):
        os.makedirs(path_to_events)

    if not os.path.exists(path_to_lighter_events):
        os.makedirs(path_to_lighter_events)

    """
    for i_day in range(1, 31) :

        day = "{}{:02d}".format(yearmonth, i_day)

        path_to_full_CSV = download_files(path_to_full_CSVs=path_to_events, day=day, data_type="export.CSV")
        #print_geotype(path_to_full_CSV)

        prepare_ligther_events_file(path_to_full_CSV, path_to_lighter_events)
    """

    # Writing cql commands file to import one day of data
    txt = prepare_cql_commands("{}/{}*_export_full.csv".format(path_to_lighter_events, yearmonth))
    with open(os.path.join(path_to_lighter_events, "cql_cmds"), "w") as my_f:
        my_f.write(txt)

