import os
import pandas as pd
from tqdm import tqdm


DATA_PATH = 'input/bp_stats.xlsx'


try:
    os.mkdir('output/datapoints')
except FileExistsError:
    pass


class DataDetailed:
    def __init__(self, data_path, out_path):

        # path to xlsx file
        self.data_path = data_path
        self.out_path = out_path

        # list of sheets
        self.sheets = [
            "Biofuels Production - Kboed",
            "Biofuels Production - Ktoe",
            "Carbon Dioxide Emissions",
            "Coal - Prices",
            "Coal - Reserves",
            "Coal Consumption - Mtoe",
            "Coal Production - Mtoe",
            "Coal Production - Tonnes",
            "Electricity Generation ",
            "Gas - Prices ",
            "Gas - Proved reserves",
            "Gas - Proved reserves history ",
            "Gas Consumption - Bcf",
            "Gas Consumption - Bcm",
            "Gas Consumption - Mtoe",
            "Gas Production - Bcf",
            "Gas Production - Bcm",
            "Gas Production - Mtoe",
            "Geo Biomass Other - Mtoe",
            "Geo Biomass Other - TWh",
            "Geothermal Capacity",
            "Hydro Consumption - Mtoe",
            "Hydro Generation - TWh",
            "Nuclear Consumption - Mtoe",
            "Nuclear Generation - TWh",
            "Oil - Proved reserves",
            "Oil - Proved reserves history",
            "Oil - Refinery throughput",
            "Oil - Refining capacity",
            "Oil - Spot crude prices",
            "Oil Consumption - Barrels",
            "Oil Consumption - Tonnes",
            "Oil Production - Barrels",
            "Oil Production - Tonnes",
            "Primary Energy Consumption",
            "Renewables - Mtoe",
            "Renewables - TWh",
            "Solar Capacity",
            "Solar Consumption - Mtoe",
            "Solar Generation - TWh",
            "Wind Capacity",
            "Wind Consumption - Mtoe",
            "Wind Generation - TWh ",
            "Elec Gen from Coal", #
            "Elec Gen from Gas",
            "Elec Gen from Oil",
            "Elec Gen from Other",
            "Graphite Production-Reserves",
            "Lithium Production-Reserves",
            "Oil Consumption - Mtoe",
            "Primary Energy - Cons capita",
            "Rare Earth Production-Reserves",
            "Cobalt Production-Reserves",
            "Elec Gen by fuel",
            "Primary Energy - Cons by fuel",
            "Renewables Generation by source",
            "Cobalt and Lithium - Prices",
            "Oil - Crude prices since 1861"
        ]


        #sheets with custom skiprow argument
        self.names_custom_start_row = {
            "Geothermal Capacity": 3,
            "Solar Capacity": 3,
            "Wind Capacity": 3
        }

        self.multiple_variables = {
            "Cobalt Production-Reserves": [
                "Cobalt Production-Reserves - Production",
                "Cobalt Production-Reserves - Reserves"
            ],
            "Elec Gen by fuel": [
                "Elec Gen by fuel - Oil", "Elec Gen by fuel - Natural Gas",
                "Elec Gen by fuel - Coal", "Elec Gen by fuel - Nuclear energy",
                "Elec Gen by fuel - Hydro electric", "Elec Gen by fuel - Renewables",
                "Elec Gen by fuel - Other #", "Elec Gen by fuel - Total"
            ],
            "Primary Energy - Cons by fuel": [
                "Primary Energy - Cons by fuel - Oil",
                "Primary Energy - Cons by fuel - Natural Gas",
                "Primary Energy - Cons by fuel - Coal",
                "Primary Energy - Cons by fuel - Nuclear energy",
                "Primary Energy - Cons by fuel - Hydro electric",
                "Primary Energy - Cons by fuel - Renewables",
                "Primary Energy - Cons by fuel - Total"
            ],
            "Renewables Generation by source": [
                "Renewables Generation by source - Wind",
                "Renewables Generation by source - Solar",
                "Renewables Generation by source - Other renewables+",
                "Renewables Generation by source - Total"
            ],
            "Cobalt and Lithium - Prices": [
                "Cobalt and Lithium - Prices - Cobalt",
                "Cobalt and Lithium - Prices - Lithium Carbonate"
            ]
        }


    def normalize_country(self, row):
        row['country'] = row['country'].str.replace(r'\s*[^A-Za-z\s]*$', '')
        return row

    # if custom is True then we use names_custom_index dict
    def process_sheet(self, sheet_id, sh, skiprows, custom=False):

        data = pd.read_excel(
            self.data_path,
            na_values=['n/a'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        index_name = "Total proved reserves" if custom else data.columns[0]
        data = data.set_index(index_name)
        data = data[:'Total World']
        last_year = data.columns.get_loc(2018)
        data = data[data.columns[:last_year+1]]
        d = data.T.unstack().reset_index()
        d.rename(columns={
            d.columns[0]: "country",
            d.columns[1]: "year",
            d.columns[2]: "value"
        }, inplace=True)
        d['year'] = d['year'].astype(str).str.replace("at end ", '').str.lstrip()

        d = self.normalize_country(d)
        d.dropna(how='any').to_csv(self.out_path+"datapoints_%s.csv" % str(sheet_id), index=False)

    def process_gas_prices(self, sheet_id, sh, skiprows):

        data = pd.read_excel(
            self.data_path,
            na_values=['n/a', '-'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        data = data[[data.columns[0]] + [x for x in data.columns[1:] if "Unnamed" not in x]]
        index_name = data.columns[0]

        data = data.set_index(index_name)
        data = data.dropna(how='any')
        data = data.iloc[1::]
        d = data.transpose().T.unstack().reset_index()
        d.rename(columns={
            d.columns[0]: "country",
            d.columns[1]: "year",
            d.columns[2]: "value"
        }, inplace=True)
        d['year'] = d['year'].astype(str).str.replace("at end ", '').str.lstrip()
        d = self.normalize_country(d)
        d.dropna(how='any').to_csv(self.out_path+"datapoints_%s.csv" % str(sheet_id), index=False)

    def process_coal_prices(self, sheet_id, sh, skiprows):

        data = pd.read_excel(
            self.data_path,
            na_values=['n/a', '-'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        data = data[[x for x in data.columns if "Unnamed" not in x and x != '   ']]

        index_name = data.columns[0]

        data = data.set_index(index_name)
        data = data.dropna(how='any').transpose().T.unstack().reset_index()
        data.rename(columns={
            data.columns[0]: "country",
            data.columns[1]: "year",
            data.columns[2]: "value"
        }, inplace=True)
        data['year'] = data['year'].astype(str).str.replace("at end ", '').str.lstrip()
        data = self.normalize_country(data)
        data.dropna(how='any').to_csv(
            self.out_path+"datapoints_%s.csv" % str(sheet_id),
            index=False
        )

    def process_proved_reserves(self, sheet_id, sh, skiprows):

        data = pd.read_excel(
            self.data_path,
            na_values=['n/a'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        index_name = data.columns[0]
        data = data.loc[3:]
        data = data.set_index(index_name)

        data = data[[x for x in data.columns if "at end" in x]]
        data = data[:'Total World']
        d = data.T.unstack().reset_index()
        d.rename(columns={
            d.columns[0]: "country",
            d.columns[1]: "year",
            d.columns[2]: "value"
        }, inplace=True)
        d['year'] = d['year'].astype(str).str.replace("at end ", '').str.lstrip()
        d = self.normalize_country(d)
        d.dropna(how='any').to_csv(self.out_path+"datapoints_%s.csv" % str(sheet_id), index=False)

    def process_crude_prices(self, sheet_id, sh, skiprows):

        data = pd.read_excel(
            self.data_path,
            na_values=['n/a', '-'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        index_name = data.columns[0]
        data = data.loc[3:49]
        data = data.set_index(index_name)
        data = data[[x for x in data.columns if "Unnamed" not in x]]
        data = data.transpose().T.unstack().reset_index()
        data.rename(columns={
            data.columns[0]: "country",
            data.columns[1]: "year",
            data.columns[2]: "value"
        }, inplace=True)
        data['year'] = data['year'].astype(str).str.replace("at end ", '').str.lstrip()
        data = self.normalize_country(data)
        data.dropna(how='any').to_csv(
            self.out_path+"datapoints_%s.csv" % str(sheet_id),
            index=False
        )

    def process_coal_reserves(self, sh, skiprows):
        data = pd.read_excel(
            self.data_path,
            na_values=['n/a'],
            sheet_name=sh,
            skiprows=skiprows
        )
        data = data.dropna(how='all')
        data['year'] = "2018"
        data = data.loc[1:53]
        for i, ch in enumerate("567"):
            data.rename(columns={
                data.columns[0]: "country",
                data.columns[i+1]: "value"
            }, inplace=True)
            data = self.normalize_country(data)
            data[['country', 'year', data.columns[i+1]]].dropna(how='any').to_csv(
                self.out_path+"datapoints_%s.csv" % ch,
                index=False
            )
            data.rename(columns={data.columns[i+1]: "%s" % ch}, inplace=True)

    def cobalt_production_reserves(self, sh, skiprows, variables_df):


        for x in range(len(self.multiple_variables[sh])):

            if x == 0:
                data = pd.read_excel(
                    self.data_path,
                    na_values=['n/a'],
                    sheet_name=sh,
                    skiprows=skiprows
                )

                data = data.dropna(how='all')
                index_name = data.columns[0]
                data_production = data.set_index(index_name)
                data_production = data_production[:'Total World']
                last_year = data_production.columns.get_loc(2018)
                d_production = data_production[data_production.columns[:last_year+1]]
                d_production = d_production.T.unstack().reset_index()
                d_production.rename(columns={
                    d_production.columns[0]: "country",
                    d_production.columns[1]: "year",
                    d_production.columns[2]: "value"
                }, inplace=True)
                d_production['year'] = (
                    d_production['year']
                    .astype(str)
                    .str.replace("at end ", '')
                    .str.lstrip()
                )
                d_production = self.normalize_country(d_production)
                sheet_id = (
                    variables_df[variables_df['name'] == \
                    self.multiple_variables[sh][x]]['id'].values[0]
                )
                d_production.dropna(how='any').to_csv(
                    self.out_path+"datapoints_%s.csv" % str(sheet_id),
                    index=False
                )
            else:
                data = pd.read_excel(
                    self.data_path,
                    na_values=['n/a'],
                    sheet_name=sh,
                    skiprows=skiprows
                )

                data = data.dropna(how='all')
                data = data[[data.columns[0], '2018.3']]
                index_name = data.columns[0]
                data_production = data.set_index(index_name)
                data_production = data_production[:'Total World']

                data_production = data_production.T.unstack().reset_index()
                data_production.rename(columns={
                    data_production.columns[0]: "country",
                    data_production.columns[1]: "year",
                    data_production.columns[2]: "value"
                }, inplace=True)
                data_production['year'] = '2018'
                data_production = self.normalize_country(data_production)
                sheet_id = (
                    variables_df[variables_df['name'] == \
                    self.multiple_variables[sh][x]]['id'].values[0]
                )
                data_production.dropna(how='any').to_csv(
                    self.out_path+"datapoints_%s.csv" % str(sheet_id),
                    index=False
                )


    def by_fuel(self, sh, skiprows, variables_df, num_of_columns=9):

        data = pd.read_excel(
            DATA_PATH,
            na_values=['n/a'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')
        data.rename(columns={
            "Renew- ables": "Renewables",
            "Renew- ables.1": "Renewables.1"
        }, inplace=True)
        index_name = data.columns[0]
        data = data.set_index(index_name)
        data = data[:'Total World']
        data = data.reset_index()



        for i in range(1, num_of_columns):
            d = data[[data.columns[0], data.columns[i]]]
            d['year'] = '2017'
            d2 = data[[data.columns[0], data.columns[i+num_of_columns-1]]]
            d2['year'] = '2018'
            d2.columns = d2.columns.str.replace(".1", "")

            sheet_id = (
                variables_df[variables_df['name'] == sh + " - " + data.columns[i]]['id'].values[0]
            )

            res = pd.concat([d, d2], ignore_index=True)
            res.rename(columns={
                res.columns[0]: "country",
                res.columns[1]: "value",
                res.columns[2]: "year"
            }, inplace=True)

            res = self.normalize_country(res)
            res.dropna(how='any').to_csv(
                self.out_path+"datapoints_%s.csv" % str(sheet_id),
                index=False
            )


    def no_countries(self, sh, skiprows, variables_df, max_row=19):

        data = pd.read_excel(
            DATA_PATH,
            na_values=['n/a'],
            sheet_name=sh,
            skiprows=skiprows
        )

        data = data.dropna(how='all')

        data = data.iloc[:max_row]
        i = 0

        if sh in self.multiple_variables:

            for x in self.multiple_variables[sh]:

                sheet_id = variables_df[variables_df['name'] == x]['id'].values[0]
                d = pd.DataFrame()
                d['country'] = 'Total World'

                d['year'] = data[data.columns[0]]
                d['year'] = d['year'].astype(str).str.replace("at end ", '').str.lstrip()
                d['value'] = data[data.columns[i+1]]
                d['country'] = 'Total World'
                d.dropna(how='any').to_csv(
                    self.out_path+"datapoints_%s.csv" % str(sheet_id),
                    index=False
                )
                i += 1
        else:
            sheet_id = variables_df[variables_df['name'] == sh]['id'].values[0]
            d = pd.DataFrame()
            d['country'] = 'Total World'

            d['year'] = data[data.columns[0]]
            d['year'] = d['year'].astype(str).str.replace("at end ", '').str.lstrip()
            d['value'] = data[data.columns[i+1]]
            d['country'] = 'Total World'
            d.dropna(how='any').to_csv(
                self.out_path+"datapoints_%s.csv" % str(sheet_id),
                index=False
            )
            i += 1


variables_df = pd.read_csv('output/variables.csv')

dat = DataDetailed(DATA_PATH, "output/datapoints/")

for sh in tqdm(dat.sheets):

    if sh == "Coal - Reserves":
        dat.process_coal_reserves(sh, 3)
    elif sh == "Cobalt Production-Reserves":
        dat.cobalt_production_reserves(sh, 2, variables_df)

    elif sh == "Elec Gen by fuel":
        dat.by_fuel(sh, 2, variables_df)

    elif sh == "Primary Energy - Cons by fuel":
        dat.by_fuel(sh, 2, variables_df, 8)
    elif sh == "Renewables Generation by source":
        dat.by_fuel(sh, 2, variables_df, 5)
    elif sh == "Cobalt and Lithium - Prices":
        dat.no_countries(sh, 4, variables_df)

    elif sh == "Oil - Crude prices since 1861":
        dat.no_countries(sh, 3, variables_df, 158)

    else:
        id_val = variables_df[variables_df['name'] == sh]['id'].values[0]
        if sh == "Coal - Prices":
            dat.process_coal_prices(id_val, sh, 1)
        elif sh == "Gas - Prices ":
            dat.process_gas_prices(id_val, sh, 3)
        elif sh in ("Gas - Proved reserves", "Oil - Proved reserves"):
            dat.process_proved_reserves(id_val, sh, 1)
        elif sh == "Oil - Spot crude prices":
            dat.process_crude_prices(id_val, sh, 1)
        elif sh in dat.names_custom_start_row:
            dat.process_sheet(id_val, sh, dat.names_custom_start_row[sh], custom=False)
        else:
            dat.process_sheet(id_val, sh, 2, custom=False)
