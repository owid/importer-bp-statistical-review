import pandas as pd
import numpy as np


LAST_YEAR_OF_DATA = 2018
DATA_PATH = "input/bp_data_2019.xlsx"
OUT_PATH = "output/datapoints/"


def normalize_country(data):
    data["country"] = data["country"].str.replace(r"\s*[^A-Za-z\s]*$", "")
    return data


def process_sheet(sheet, skiprows, var_id):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data = data.set_index(data.columns[0])
    data = data[:"Total World"]
    last_year = data.columns.get_loc(LAST_YEAR_OF_DATA)
    data = data[data.columns[:last_year+1]]
    data = data.T.unstack().reset_index()
    data = data.rename(columns={
        data.columns[0]: "country",
        data.columns[1]: "year",
        data.columns[2]: "value"
    })
    data["year"] = data["year"].astype(str).str.replace("at end ", "").str.lstrip()
    data = normalize_country(data)
    data.dropna(how="any").to_csv(f"{OUT_PATH}datapoints_{var_id}.csv", index=False)


def process_coal_prices(sheet, skiprows, var_id):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a", "-"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data = data[[x for x in data.columns if "Unnamed" not in x and x != "   "]]
    data = data.set_index(data.columns[0])
    data = data.dropna(how="any").transpose().T.unstack().reset_index()
    data = data.rename(columns={
        data.columns[0]: "country",
        data.columns[1]: "year",
        data.columns[2]: "value"
    })
    data["year"] = data["year"].astype(str).str.replace("at end ", "").str.lstrip()
    data = normalize_country(data)
    data.dropna(how="any").to_csv(f"{OUT_PATH}datapoints_{var_id}.csv", index=False)


def process_coal_reserves(sheet, skiprows, var_ids):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data["year"] = LAST_YEAR_OF_DATA
    data = data.set_index(data.columns[0])
    data = data[:"Total World"]
    data = data.reset_index(drop=False)
    for i, var_id in enumerate(var_ids, 1):
        tmp = data.rename(columns={
            data.columns[0]: "country",
            data.columns[i]: "value"
        })
        tmp = normalize_country(tmp)
        tmp[["country", "year", tmp.columns[i]]].dropna(how="any").to_csv(
            f"{OUT_PATH}datapoints_{var_id}.csv",
            index=False
        )


def cobalt_production_reserves(sheet, skiprows, var_ids):

    for i, var_id in enumerate(var_ids):

        if i == 0:
            data = pd.read_excel(
                DATA_PATH,
                na_values=["n/a"],
                sheet_name=sheet,
                skiprows=skiprows
            )

            data = data.dropna(how="all")
            index_name = data.columns[0]
            data_production = data.set_index(index_name)
            data_production = data_production[:"Total World"]
            last_year = data_production.columns.get_loc(LAST_YEAR_OF_DATA)
            d_production = data_production[data_production.columns[:last_year+1]]
            d_production = d_production.T.unstack().reset_index()
            d_production = d_production.rename(columns={
                d_production.columns[0]: "country",
                d_production.columns[1]: "year",
                d_production.columns[2]: "value"
            })
            d_production["year"] = (
                d_production["year"]
                .astype(str)
                .str.replace("at end ", "")
                .str.lstrip()
            )
            d_production = normalize_country(d_production)
            d_production.dropna(how="any").to_csv(
                f"{OUT_PATH}datapoints_{var_id}.csv",
                index=False
            )
        else:
            data = pd.read_excel(
                DATA_PATH,
                na_values=["n/a"],
                sheet_name=sheet,
                skiprows=skiprows
            )

            data = data.dropna(how="all")
            data = data[[data.columns[0], "2018.3"]]
            index_name = data.columns[0]
            data_production = data.set_index(index_name)
            data_production = data_production[:"Total World"]

            data_production = data_production.T.unstack().reset_index()
            data_production = data_production.rename(columns={
                data_production.columns[0]: "country",
                data_production.columns[1]: "year",
                data_production.columns[2]: "value"
            })
            data_production["year"] = LAST_YEAR_OF_DATA
            data_production = normalize_country(data_production)
            data_production.dropna(how="any").to_csv(
                f"{OUT_PATH}datapoints_{var_id}.csv",
                index=False
            )


def no_countries(sheet, skiprows, var_id):

    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a"],
        sheet_name=sheet,
        skiprows=skiprows
    )

    data = data.dropna(how="all")
    max_row = data.iloc[:, 0].str.len().isnull().idxmin()
    data = data.iloc[:max_row]

    if isinstance(var_id, np.ndarray):
        for i, sheet_id in enumerate(var_id, 1):
            tmp = pd.DataFrame()
            tmp["country"] = "Total World"
            tmp["year"] = data[data.columns[0]]
            tmp["year"] = tmp["year"].astype(str).str.replace("at end ", "").str.lstrip()
            tmp["value"] = data[data.columns[i]]
            tmp["country"] = "Total World"
            tmp.dropna(how="any").to_csv(
                f"{OUT_PATH}datapoints_{sheet_id}.csv",
                index=False
            )

    else:
        tmp = pd.DataFrame()
        tmp["country"] = "Total World"

        tmp["year"] = data[data.columns[0]]
        tmp["year"] = tmp["year"].astype(str).str.replace("at end ", "").str.lstrip()
        tmp["value"] = data[data.columns[1]]
        tmp["country"] = "Total World"
        tmp.dropna(how="any").to_csv(
            f"{OUT_PATH}datapoints_{var_id}.csv",
            index=False
        )


def by_fuel(sheet, skiprows, var_ids):

    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a"],
        sheet_name=sheet,
        skiprows=skiprows
    )

    data = data.dropna(how="all")
    data = data.rename(columns={
        "Renew- ables": "Renewables",
        "Renew- ables.1": "Renewables.1"
    })
    index_name = data.columns[0]
    data = data.set_index(index_name)
    data = data[:"Total World"]
    data = data.reset_index()

    for i, var_id in enumerate(var_ids, 1):

        part_a = data.iloc[:, [0, i]].copy()
        part_a.loc[:, "year"] = LAST_YEAR_OF_DATA - 1

        part_b = data.iloc[:, [0, i+len(var_ids)]].copy()
        part_b.loc[:, "year"] = LAST_YEAR_OF_DATA
        part_b.columns = part_b.columns.str.replace(".1", "")

        res = pd.concat([part_a, part_b], ignore_index=True)
        res = res.rename(columns={
            res.columns[0]: "country",
            res.columns[1]: "value",
            res.columns[2]: "year"
        })

        res = normalize_country(res)
        res.dropna(how="any").to_csv(
            f"{OUT_PATH}datapoints_{var_id}.csv",
            index=False
        )


def process_gas_prices(sheet, skiprows, var_id):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a", "-"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data = data[[data.columns[0]] + [x for x in data.columns[1:] if "Unnamed" not in x]]
    data = data.set_index(data.columns[0])
    data = data.dropna(how="any")
    data = data[~data.index.isnull()]
    data = data.transpose().T.unstack().reset_index()
    data = data.rename(columns={
        data.columns[0]: "country",
        data.columns[1]: "year",
        data.columns[2]: "value"
    })
    data["year"] = data["year"].astype(str).str.replace("at end ", "").str.lstrip()
    data = normalize_country(data)
    data.dropna(how="any").to_csv(f"{OUT_PATH}datapoints_{var_id}.csv", index=False)


def process_proved_reserves(sheet, skiprows, var_id):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data = data.set_index(data.columns[0])
    data = data[~data.index.isnull()]
    data = data[[x for x in data.columns if "at end" in x]]
    data = data[:"Total World"]
    data = data.T.unstack().reset_index()
    data = data.rename(columns={
        data.columns[0]: "country",
        data.columns[1]: "year",
        data.columns[2]: "value"
    })
    data["year"] = data["year"].astype(str).str.replace("at end ", "").str.lstrip()
    data = normalize_country(data)
    data.dropna(how="any").to_csv(f"{OUT_PATH}datapoints_{var_id}.csv", index=False)


def process_crude_prices(sheet, skiprows, var_id):
    data = pd.read_excel(
        DATA_PATH,
        na_values=["n/a", "-"],
        sheet_name=sheet,
        skiprows=skiprows
    )
    data = data.dropna(how="all")
    data = data.set_index(data.columns[0])
    data = data[data.index.astype(str).str.match(r"\d{4}")]
    data = data[[x for x in data.columns if "Unnamed" not in x]]
    data = data.transpose().T.unstack().reset_index()
    data = data.rename(columns={
        data.columns[0]: "country",
        data.columns[1]: "year",
        data.columns[2]: "value"
    })
    data["year"] = data["year"].astype(str).str.replace("at end ", "").str.lstrip()
    data = normalize_country(data)
    data.dropna(how="any").to_csv(
        f"{OUT_PATH}datapoints_{var_id}.csv",
        index=False
    )


def main():
    metadata = pd.read_csv("input/workbook_format.csv")
    metadata.loc[metadata.subvariable.isnull(), "subvariable"] = metadata.sheet_name
    metadata = metadata.merge(
        pd.read_csv("output/variables.csv"),
        left_on=["subvariable"],
        right_on=["name"]
    )

    #sheets with custom skiprow argument
    names_custom_start_row = {
        "Geothermal Capacity": 3,
        "Solar Capacity": 3,
        "Wind Capacity": 3
    }

    for sheet_name in sorted(metadata.sheet_name.unique()):

        var_id = metadata.loc[metadata.sheet_name == sheet_name, "id"].values
        if len(var_id) == 1:
            var_id = var_id[0]

        skiprows = metadata.loc[metadata.sheet_name == sheet_name, "skiprows"].values[0]

        print(f"Processing sheet '{sheet_name}' for variable(s) {var_id}")

        if sheet_name == "Coal - Reserves":
            process_coal_reserves(sheet_name, skiprows, var_id)
        elif sheet_name == "Cobalt Production-Reserves":
            cobalt_production_reserves(sheet_name, skiprows, var_id)
        elif sheet_name in (
                "Elec Gen by fuel",
                "Primary Energy - Cons by fuel",
                "Renewables Generation by source"
            ):
            by_fuel(sheet_name, skiprows, var_id)
        elif sheet_name == "Cobalt and Lithium - Prices":
            no_countries(sheet_name, skiprows, var_id)
        elif sheet_name == "Oil - Crude prices since 1861":
            no_countries(sheet_name, 3, var_id)
        elif sheet_name == "Coal - Prices":
            process_coal_prices(sheet_name, skiprows, var_id)
        elif sheet_name == "Gas - Prices ":
            process_gas_prices(sheet_name, 3, var_id)
        elif sheet_name in ("Gas - Proved reserves", "Oil - Proved reserves"):
            process_proved_reserves(sheet_name, 1, var_id)
        elif sheet_name == "Oil - Spot crude prices":
            process_crude_prices(sheet_name, 1, var_id)
        elif sheet_name in names_custom_start_row:
            process_sheet(sheet_name, names_custom_start_row[sheet_name], var_id)
        else:
            process_sheet(sheet_name, skiprows, var_id)


if __name__ == "__main__":
    main()
