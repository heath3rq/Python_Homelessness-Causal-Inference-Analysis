import pandas as pd
import numpy as np


# 1. Processing HIC

fips_coc = {
    "36005": "NY-600",
    "36047": "NY-600",
    "36061": "NY-600",
    "36081": "NY-600",
    "36085": "NY-600",
    #  LA CoC excludes the cities of Glendale, Pasadena and Long Beach
    "06037": "CA-600",
    "26163": "MI-501",
    "48201": "TX-700",
    "48339": "TX-700",
    "48157": "TX-700",
    "08001": "CO-503",
    "08005": "CO-503",
    "08013": "CO-503",
    "08014": "CO-503",
    "08031": "CO-503",
    "08035": "CO-503",
    "08059": "CO-503",
    "04013": "AZ-502",
    "53033": "WA-500",
    "42101": "PA-500",
    "32003": "NV-500",
    "06073": "CA-601",
    "06085": "CA-500",
    "06071": "CA-609",
    "05049": "GA-500",
    "12057": "FL-501",
    "17031": "IL-510",
    "06075": "CA-501",
    "11001": "DC-500",
    "48453": "TX-503",
    "25025": "MA-500",
}
uniquecoc = set(val for val in fips_coc.values())


hic_path = "https://www.huduser.gov/portal/sites/default/files/xls/2007-2022-HIC-Counts-by-CoC.xlsx"
hic_2022 = pd.read_excel(hic_path, sheet_name="2022", header=1)
hic_2021 = pd.read_excel(hic_path, sheet_name="2021", header=1)
hic_2020 = pd.read_excel(hic_path, sheet_name="2020", header=1)
hic_2019 = pd.read_excel(hic_path, sheet_name="2019", header=1)
hic_2018 = pd.read_excel(hic_path, sheet_name="2018", header=1)
hic_2017 = pd.read_excel(hic_path, sheet_name="2017", header=1)
hic_2016 = pd.read_excel(hic_path, sheet_name="2016", header=1)
hic_2015 = pd.read_excel(hic_path, sheet_name="2015", header=1)
hic_2014 = pd.read_excel(hic_path, sheet_name="2014", header=1)
hic_2013 = pd.read_excel(hic_path, sheet_name="2013", header=1)
hic_2012 = pd.read_excel(hic_path, sheet_name="2012", header=1)
hic_2011 = pd.read_excel(hic_path, sheet_name="2011", header=1)
hic_2010 = pd.read_excel(hic_path, sheet_name="2010", header=1)
hic_2009 = pd.read_excel(hic_path, sheet_name="2009", header=1)
hic_2008 = pd.read_excel(hic_path, sheet_name="2008", header=1)
hic_2007 = pd.read_excel(hic_path, sheet_name="2007", header=1)


def process_hic_1(df: object, year: int) -> object:
    """Process Dataframe from 2014-2022."""
    assert (df["CoC Number"].duplicated() == True).any() == False
    df["year"] = year
    df["permanent_housing"] = (
        df["Total Year-Round Beds (PSH)"] + df["Total Year-Round Beds (OPH)"]
    )
    hic_ph = df.loc[
        df["CoC Number"].isin(uniquecoc), ["CoC Number", "year", "permanent_housing"]
    ].reset_index(drop="index")
    return hic_ph


def process_hic_2(df: object, year: int) -> object:
    """Process Dataframe for 2013."""
    assert (df["CoC Number"].duplicated() == True).any() == False
    df["year"] = year
    df["permanent_housing"] = df["Total PSH Beds"]
    hic_ph = df.loc[
        df["CoC Number"].isin(uniquecoc), ["CoC Number", "year", "permanent_housing"]
    ].reset_index(drop="index")
    return hic_ph


def process_hic_3(df: object, year: int) -> object:
    """Process Dataframe for 2007-2012."""
    assert (df["CoC"].duplicated() == True).any() == False
    df["year"] = year
    df["permanent_housing"] = df["Total PSH Beds"]
    hic_ph = (
        df.loc[df["CoC"].isin(uniquecoc), ["CoC", "year", "permanent_housing"]]
        .reset_index(drop="index")
        .rename(columns={"CoC": "CoC Number"})
    )
    return hic_ph


hic_2022 = process_hic_1(hic_2022, 2022)
hic_2021 = process_hic_1(hic_2021, 2021)
hic_2020 = process_hic_1(hic_2020, 2020)
hic_2019 = process_hic_1(hic_2019, 2019)
hic_2018 = process_hic_1(hic_2018, 2018)
hic_2017 = process_hic_1(hic_2017, 2017)
hic_2016 = process_hic_1(hic_2016, 2016)
hic_2015 = process_hic_1(hic_2015, 2015)
hic_2014 = process_hic_1(hic_2014, 2014)
hic_2013 = process_hic_2(hic_2013, 2013)
hic_2012 = process_hic_3(hic_2012, 2012)
hic_2011 = process_hic_3(hic_2011, 2011)
hic_2010 = process_hic_3(hic_2010, 2010)
hic_2009 = process_hic_3(hic_2009, 2009)
hic_2008 = process_hic_3(hic_2008, 2008)
hic_2007 = process_hic_3(hic_2007, 2007)


# from functools import reduce

# # define list of DataFrames
hic = [
    hic_2022,
    hic_2021,
    hic_2020,
    hic_2019,
    hic_2018,
    hic_2017,
    hic_2016,
    hic_2015,
    hic_2014,
    hic_2013,
    hic_2012,
    hic_2011,
    hic_2010,
    hic_2009,
    hic_2008,
    hic_2007,
]

# Concatenate the dataframes
hic_combined = pd.concat(hic, ignore_index=True)

# Confirm the resulting dataframe has no duplicates and null values


def check_dup_null(df: object) -> None:
    assert df.duplicated().sum() == 0, "Found duplicates in the dataframe."
    assert df.isnull().sum().sum() == 0, "Found null values in the dataframe."
    assert (
        df.groupby("CoC Number").count() == 16
    ).all().all() == True, (
        "Certain years are missing. Review yearly data for each CoC."  # 16 years
    )


check_dup_null(hic_combined)


# #### 2. Processing PIT


pit_path = "https://www.huduser.gov/portal/sites/default/files/xls/2007-2022-PIT-Counts-by-CoC.xlsx"
pit_2022 = pd.read_excel(
    pit_path, sheet_name="2022", usecols="A,D", skipfooter=3
).rename(columns={"Overall Homeless, 2022": "homeless"})
pit_2021 = pd.read_excel(
    pit_path, sheet_name="2021", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2021": "homeless"})
pit_2020 = pd.read_excel(
    pit_path, sheet_name="2020", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2020": "homeless"})
pit_2019 = pd.read_excel(
    pit_path, sheet_name="2019", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2019": "homeless"})
pit_2018 = pd.read_excel(
    pit_path, sheet_name="2018", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2018": "homeless"})
pit_2017 = pd.read_excel(
    pit_path, sheet_name="2017", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2017": "homeless"})
pit_2016 = pd.read_excel(
    pit_path, sheet_name="2016", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2016": "homeless"})
pit_2015 = pd.read_excel(
    pit_path, sheet_name="2015", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2015": "homeless"})
pit_2014 = pd.read_excel(
    pit_path, sheet_name="2014", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2014": "homeless"})
pit_2013 = pd.read_excel(
    pit_path, sheet_name="2013", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2013": "homeless"})
pit_2012 = pd.read_excel(
    pit_path, sheet_name="2012", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2012": "homeless"})
pit_2011 = pd.read_excel(
    pit_path, sheet_name="2011", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2011": "homeless"})
pit_2010 = pd.read_excel(
    pit_path, sheet_name="2010", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2010": "homeless"})
pit_2009 = pd.read_excel(
    pit_path, sheet_name="2009", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2009": "homeless"})
pit_2008 = pd.read_excel(
    pit_path, sheet_name="2008", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2008": "homeless"})
pit_2007 = pd.read_excel(
    pit_path, sheet_name="2007", usecols="A,C", skipfooter=3
).rename(columns={"Overall Homeless, 2007": "homeless"})


def process_pit(df: object, year: int) -> object:
    """Process Dataframe for 2007-2012."""
    assert (df["CoC Number"].duplicated() == True).any() == False
    pit_df = (
        df.loc[df["CoC Number"].isin(uniquecoc)]
        .reset_index(drop="index")
        .rename(columns={"CoC": "CoC Number"})
    )
    pit_df["year"] = year
    return pit_df


pit_2022 = process_pit(pit_2022, 2022)
pit_2021 = process_pit(pit_2021, 2021)
pit_2020 = process_pit(pit_2020, 2020)
pit_2019 = process_pit(pit_2019, 2019)
pit_2018 = process_pit(pit_2018, 2018)
pit_2017 = process_pit(pit_2017, 2017)
pit_2016 = process_pit(pit_2016, 2016)
pit_2015 = process_pit(pit_2015, 2015)
pit_2014 = process_pit(pit_2014, 2014)
pit_2013 = process_pit(pit_2013, 2013)
pit_2012 = process_pit(pit_2012, 2012)
pit_2011 = process_pit(pit_2011, 2011)
pit_2010 = process_pit(pit_2010, 2010)
pit_2009 = process_pit(pit_2009, 2009)
pit_2008 = process_pit(pit_2008, 2008)
pit_2007 = process_pit(pit_2007, 2007)


# Define list of DataFrames
pit = [
    pit_2022,
    pit_2021,
    pit_2020,
    pit_2019,
    pit_2018,
    pit_2017,
    pit_2016,
    pit_2015,
    pit_2014,
    pit_2013,
    pit_2012,
    pit_2011,
    pit_2010,
    pit_2009,
    pit_2008,
    pit_2007,
]

# Concatenate the dataframes
pit_combined = pd.concat(pit, ignore_index=True)

# # Confirm the resulting dataframe has no duplicates and null values
check_dup_null(pit_combined)


# ##### 2.5 Merge HIC and PIT


df_hic_pit = pd.merge(
    hic_combined, pit_combined, on=["CoC Number", "year"], how="outer", validate="1:1"
)

check_dup_null(df_hic_pit)


# #### 3. Poverty Data


poverty = pd.read_excel(
    "https://www2.census.gov/programs-surveys/saipe/datasets/time-series/model-tables/allpovu.xls",
    header=[3],
    usecols="A:D, E, I, M, Q, U, Y, AC, AG, AK, AO, AS, AW, BA, BE, BI",
)
poverty = poverty.rename(
    columns={
        "Poverty Universe, All Ages": "2021",
        "Poverty Universe, All Ages.1": 2020,
        "Poverty Universe, All Ages.2": 2019,
        "Poverty Universe, All Ages.3": 2018,
        "Poverty Universe, All Ages.4": 2017,
        "Poverty Universe, All Ages.5": 2016,
        "Poverty Universe, All Ages.6": 2015,
        "Poverty Universe, All Ages.7": 2014,
        "Poverty Universe, All Ages.8": 2013,
        "Poverty Universe, All Ages.9": 2012,
        "Poverty Universe, All Ages.10": 2011,
        "Poverty Universe, All Ages.11": 2010,
        "Poverty Universe, All Ages.12": 2009,
        "Poverty Universe, All Ages.13": 2008,
        "Poverty Universe, All Ages.14": 2007,
    }
)


for i in range(len(poverty)):
    if len(str(poverty.loc[i, "State FIPS code"])) == 1:
        poverty.loc[i, "State FIPS code"] = "0" + str(poverty.loc[i, "State FIPS code"])
    else:
        poverty.loc[i, "State FIPS code"] = str(poverty.loc[i, "State FIPS code"])

for i in range(len(poverty)):
    if len(str(poverty.loc[i, "County FIPS code"])) == 1:
        poverty.loc[i, "County FIPS code"] = "00" + str(
            poverty.loc[i, "County FIPS code"]
        )
    elif len(str(poverty.loc[i, "County FIPS code"])) == 2:
        poverty.loc[i, "County FIPS code"] = "0" + str(
            poverty.loc[i, "County FIPS code"]
        )
    else:
        poverty.loc[i, "County FIPS code"] = str(poverty.loc[i, "County FIPS code"])

poverty["fips"] = poverty["State FIPS code"] + poverty["County FIPS code"]

poverty_filtered = (
    poverty[poverty.fips.isin(fips_coc.keys())]
    .drop(["State FIPS code", "County FIPS code"], axis=1)
    .reset_index(drop="index")
)
poverty_filtered["CoC Number"] = poverty_filtered["fips"].map(fips_coc)

poverty_grouped = poverty_filtered.groupby("CoC Number").apply("sum", numeric_only=True)
poverty_grouped.reset_index(inplace=True)


def melt_df(dataframe: object, col_name: str):
    # Transform the dataframe to matching format as the previous dataframes
    df_melted = pd.melt(
        dataframe, id_vars=["CoC Number"], var_name="year", value_name=col_name
    )
    # Convert the 'Year' column to integer type
    df_melted["year"] = df_melted["year"].astype(int)
    return df_melted


poverty_melted = melt_df(poverty_grouped, "poverty")


# Merge poverty data with HIC & PIT
df_hic_pit_pov = pd.merge(
    df_hic_pit, poverty_melted, on=["CoC Number", "year"], how="outer", validate="1:1"
)


# Confirm the resulting dataframe has no duplicates and null values
def check_dup_null_to2021(df: object, col: object, num_null_allowed: int) -> None:
    assert df.duplicated().sum() == 0, "Found duplicates in the dataframe."
    assert (
        df.isnull().sum().sum() == num_null_allowed
    ), "Found null values in the dataframe."
    assert (
        df.groupby("CoC Number")[["permanent_housing", "homeless"]].count() == 16
    ).all().all() == True, "Missing certain years of data for permanent housing and homeless count. Review yearly data for each CoC."
    # Because certain dataset only contains data from 2007-2021,
    # null values are expected from these data sources for 2022
    assert (
        df.groupby("CoC Number")[col].count() == 15
    ).all().all() == True, "Missing certain years of data from 2007 to 2021. Review yearly data for each CoC."


check_dup_null_to2021(df_hic_pit_pov, "poverty", 19)


# #### 4. Unemployment Data


unemployment = pd.read_excel(
    "https://www.ers.usda.gov/webdocs/DataFiles/48747/Unemployment.xlsx?v=2298.6",
    header=[4],
    usecols="A, AL, AP, AT, AX, BB, BF, BJ, BN, BR, BV, BZ, CD, CH, CL, CP",
    converters={"FIPS_code": str},
)
unemployment = unemployment.rename(
    columns={
        "FIPS_code": "fips",
        "Unemployment_rate_2007": 2007,
        "Unemployment_rate_2008": 2008,
        "Unemployment_rate_2009": 2009,
        "Unemployment_rate_2010": 2010,
        "Unemployment_rate_2011": 2011,
        "Unemployment_rate_2012": 2012,
        "Unemployment_rate_2013": 2013,
        "Unemployment_rate_2014": 2014,
        "Unemployment_rate_2015": 2015,
        "Unemployment_rate_2016": 2016,
        "Unemployment_rate_2017": 2017,
        "Unemployment_rate_2018": 2018,
        "Unemployment_rate_2019": 2019,
        "Unemployment_rate_2020": 2020,
        "Unemployment_rate_2021": 2021,
    }
)
unemployment = unemployment[unemployment.fips.isin(fips_coc.keys())].reset_index(
    drop="index"
)
unemployment["CoC Number"] = unemployment["fips"].map(fips_coc)

unemployment_grouped = unemployment.groupby("CoC Number").apply(
    "sum", numeric_only=True
)
unemployment_grouped.reset_index(inplace=True)


unemployment_melted = melt_df(unemployment_grouped, "unemployment_rate")


# Merge poverty data with HIC & PIT
df_hic_pit_pov_unemp = pd.merge(
    df_hic_pit_pov,
    unemployment_melted,
    on=["CoC Number", "year"],
    how="outer",
    validate="1:1",
)

check_dup_null_to2021(df_hic_pit_pov_unemp, ["poverty", "unemployment_rate"], 19 * 2)

df_hic_pit_pov_unemp.to_csv("HIC_PIT_POV_UNEMP_2007_2022.csv")

print("Completed: output file exported to csv.")
