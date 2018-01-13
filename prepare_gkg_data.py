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



colnames = ["GKGRECORDID","DATE","SourceCollectionIdentifier","SourceCommonName","DocumentIdentifier","Counts","V2Counts","Themes","V2Themes","Locations","V2Locations","Persons","V2Persons","Organizations","V2Organizations","V2Tone","Dates","GCAM","SharingImage","RelatedImages","SocialImageEmbeds","SocialVideoEmbeds","Quotations","AllNames","Amounts","TranslationInfo","Extras"]
cols_to_keep = ["DATE",
                "DocumentIdentifier",
                "Locations",
                "Persons"
                ]

def prepare_ligther_gkg_file(path_to_full_CSV, path_to_lighter_gkg):

    filename = os.path.split(path_to_full_CSV)[-1]
    lighterfilepath = os.path.join(path_to_lighter_gkg, filename)

    if not os.path.exists(path_to_full_CSV):
        print("File {} skipped => {} doesn't existed".format(filename, path_to_full_CSV))

    else:
        if not os.path.exists(lighterfilepath):

            df = pd.read_csv(path_to_full_CSV, sep="\t", names=colnames, low_memory=False, encoding='latin-1')
            # Writing new csv with less columns
            df.loc[:, cols_to_keep].to_csv(lighterfilepath, sep="|", index=False)
            print("Lighter gkg csv done for {}".format(filename))
        else :
            print("Lighter gkg csv {} skipped => {} already existed".format(filename, lighterfilepath))


def prepare_cql_commands(filename_pattern):

    txt = """CREATE TABLE november.gkg
(	
DATE TEXT,
DocumentIdentifier TEXT,
Locations TEXT,
Persons TEXT,

PRIMARY KEY ((DocumentIdentifier))
);

COPY november.gkg
(DATE, 
DocumentIdentifier, 
Locations,
Persons)
FROM '{}'
WITH HEADER=TRUE and DELIMITER='|';
""".format(filename_pattern)

    return txt

if __name__ == "__main__" :

    yearmonth = "201711"

    path_to_gkg = os.path.join(base_folder, "gkg")
    path_to_lighter_gkg = os.path.join(base_folder, "lighter_gkg")

    if not os.path.exists(path_to_gkg):
        os.makedirs(path_to_gkg)

    if not os.path.exists(path_to_lighter_gkg):
        os.makedirs(path_to_lighter_gkg)

    """
    for i_day in range(1, 31) :

        day = "{}{:02d}".format(yearmonth, i_day)

        path_to_full_CSV = download_files(path_to_full_CSVs=path_to_gkg, day=day, data_type="gkg.csv")
        #print_geotype(path_to_full_CSV)

        prepare_ligther_gkg_file(path_to_full_CSV, path_to_lighter_gkg)
    """

    # Writing cql commands file to import one day of data
    txt = prepare_cql_commands("{}/{}*_gkg_full.csv".format(path_to_lighter_gkg, yearmonth))
    with open(os.path.join(path_to_lighter_gkg, "cql_cmds"), "w") as my_f:
        my_f.write(txt)

