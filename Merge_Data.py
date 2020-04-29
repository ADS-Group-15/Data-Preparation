import pandas as pd
import csv

hour_filename = 'LPPH.csv'
person_filename = 'LPPP.csv'
source_filename = 'Data.csv'

def merge_csv(file1, file2, file3):
    dfH = pd.read_csv(file1, sep=",", header=0)
    dfP = pd.read_csv(file2, sep=",", header=0)
    dfS = pd.read_csv(file3, sep=",", header=0)
    dfS_Lpph_unit = dfS[(dfS['NA_ITEM'] == "Nominal labour productivity per hour worked") & (dfS['UNIT'] == "Index, 2020=100")]
    dfS_Lppp_unit = dfS[(dfS['NA_ITEM'] == "Nominal labour productivity per person") & (dfS['UNIT'] == "Index, 2020=100")]
    merge(dfH, dfS_Lpph_unit, dfS, file3)
    merge(dfP, dfS_Lppp_unit, dfS, file3)

def merge(dfFrom, dfData, dfTo, fileTo):
    for index in list(dfData.index):
        year = dfData.at[index, 'TIME']
        geo = dfData.at[index, 'GEO']
        if year > 2018:
            break
        else:
            picked_row = dfFrom[(dfFrom['geo-time'] == geo) & (dfFrom[str(year)])]
            id = int(list(picked_row.index)[0])
            new_data = picked_row.at[id, str(year)]
            print(index, " ", year, " ", geo, " ", new_data)
            dfTo.at[index, 'Value'] = new_data

    dfTo.to_csv(fileTo, index=False, quotechar='"', quoting=csv.QUOTE_ALL)


def main():
    merge_csv(hour_filename, person_filename, source_filename)

if __name__ == "__main__":
    main()