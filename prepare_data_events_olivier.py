import pandas as pd
import os, re
from pprint import pprint

base_folder = "/Users/ocalvet/Documents/MS BGD/INF728 - BDD NoSQL/Projet"

def get_filenames_for_day(day):
    with open(os.path.join(base_folder, "ls_telecom.gdelt")) as my_file:
        return re.findall(r"({}\d+\.export\.CSV\.zip)".format(day), my_file.read())

def download_files(path_to_day, filenames):
    for i, filename in enumerate(filenames):
        if not os.path.exists(os.path.join(path_to_day, filename)):
            os.system("aws s3 cp s3://telecom.gdelt/{} {}/".format(filename, path_to_day.replace(" ", "\ ")))
            print("Download {}/{} : {} done".format(i + 1, len(filenames), filename))
        else:
            print("Download {}/{} : skipped => {} already existed".format(i + 1, len(filenames), filename))

colnames = "GLOBALEVENTID;SQLDATE;MonthYear;Year;FractionDate;Actor1Code;Actor1Name;Actor1CountryCode;Actor1KnownGroupCode;Actor1EthnicCode;Actor1Religion1Code;Actor1Religion2Code;Actor1Type1Code;Actor1Type2Code;Actor1Type3Code;Actor2Code;Actor2Name;Actor2CountryCode;Actor2KnownGroupCode;Actor2EthnicCode;Actor2Religion1Code;Actor2Religion2Code;Actor2Type1Code;Actor2Type2Code;Actor2Type3Code;IsRootEvent;EventCode;EventBaseCode;EventRootCode;QuadClass;GoldsteinScale;NumMentions;NumSources;NumArticles;AvgTone;Actor1Geo_Type;Actor1Geo_FullName;Actor1Geo_CountryCode;Actor1Geo_ADM1Code;Actor1Geo_ADM2Code;Actor1Geo_Lat;Actor1Geo_Long;Actor1Geo_FeatureID;Actor2Geo_Type;Actor2Geo_FullName;Actor2Geo_CountryCode;Actor2Geo_ADM1Code;Actor2Geo_ADM2Code;Actor2Geo_Lat;Actor2Geo_Long;Actor2Geo_FeatureID;ActionGeo_Type;ActionGeo_FullName;ActionGeo_CountryCode;ActionGeo_ADM1Code;ActionGeo_ADM2Code;ActionGeo_Lat;ActionGeo_Long;ActionGeo_FeatureID;DATEADDED;SOURCEURL".split(
    ";")

cols_to_keep = ["GLOBALEVENTID",
                "SQLDATE",
                "MonthYear",
                "Actor1Geo_CountryCode",
                "Actor2Geo_CountryCode",
                "Actor1Name",
                "Actor2Name",
                "EventCode",
                "EventRootCode",
                "NumMentions"]

def prepare_ligther_files(filenames, path_to_files, path_to_lighter_day):
    for i, filename in enumerate(filenames):

        filepath = os.path.join(path_to_files, filename)
        lighterfilepath = os.path.join(path_to_lighter_day, filename.replace(".zip", ""))

        if not os.path.exists(filepath):
            print("File {}/{} : {} skipped => {} doesn't existed".format(i + 1, len(filenames), filename, filepath))

        else:
            df = pd.read_csv(filepath, sep="\t", names=colnames)
            # Writing new csv with less columns
            df.loc[:, cols_to_keep].to_csv(lighterfilepath, sep=";", index=False)
            print("File {}/{} : {} done".format(i + 1, len(filenames), filename))

def prepare_cql_commands(filename_pattern):
    txt = """CREATE TABLE events.my_day_test 
(	
GLOBALEVENTID int, 
SQLDATE int, 
MonthYear int, 
Actor1Geo_CountryCode TEXT, 
Actor2Geo_CountryCode TEXT, 
Actor1Name TEXT, 
Actor2Name TEXT,
EventCode int,
EventRootCode int,
NumMentions int,

PRIMARY KEY ((MonthYear, EventRootCode), SQLDATE, GLOBALEVENTID)
) WITH CLUSTERING ORDER BY (SQLDATE ASC);

COPY events.my_day_test 
(GLOBALEVENTID, 
SQLDATE, 
MonthYear, 
Actor1Geo_CountryCode, 
Actor2Geo_CountryCode, 
Actor1Name, 
Actor2Name, 
EventCode, 
EventRootCode, 
NumMentions)
FROM '~/lighter/{}'
WITH HEADER=TRUE and DELIMITER=';';
""".format(filename_pattern)
    return txt

if __name__ == "__main__" :
    filenames = {}
    for i_day in range(1, 32) :

        day = "201701{:02d}".format(i_day)
        path_to_zips = os.path.join(base_folder, "export")
        path_to_light_csv = os.path.join(base_folder, "lighter")

        if not os.path.exists(path_to_zips):
            os.makedirs(path_to_zips)

        if not os.path.exists(path_to_light_csv):
            os.makedirs(path_to_light_csv)

        filenames[day] = get_filenames_for_day(day)
        #pprint(filenames[day])
        print("nb files for day", day, len(filenames[day]))

        download_files(path_to_zips, filenames[day])
        prepare_ligther_files(filenames[day], path_to_zips, path_to_light_csv)


    # Writing cql commands file to import one day of data
    txt = prepare_cql_commands("201701*.export.CSV")
    with open(os.path.join(path_to_light_csv, "cql_cmds"), "w") as my_f:
        my_f.write(txt)

