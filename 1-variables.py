#!/usr/bin/env python
# coding: utf-8

# Variables.csv

# In[10]:


import pandas as pd
import numpy as np
import xlrd
import os
import re
from tqdm import tqdm


# In[11]:


class Data:
    def __init__(self, data_path):
        
        # path to xlsx file
        self.data_path = data_path
        
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
        "Cobalt and Lithium - Prices",
        "Cobalt Production-Reserves", 
        "Elec Gen by fuel", 
        "Elec Gen from Coal",
        "Elec Gen from Gas",
        "Elec Gen from Oil",
        "Elec Gen from Other",
        "Graphite Production-Reserves",
        "Lithium Production-Reserves",
        "Oil - Crude prices since 1861",
        "Oil Consumption - Mtoe",
        "Primary Energy - Cons by fuel", 
        "Primary Energy - Cons capita",
        "Rare Earth Production-Reserves",
        "Renewables Generation by source" 
            
        ]
        
        # counter for ids
        self.counter = 1

        self.names, self.units, self.notes, self.ids = [], [], [], []
        
        #sheets with custom skiprow argument
        self.names_custom_start_row = {
            "Coal - Prices": 1, 
            "Coal - Reserves": 3,
            "Gas - Prices ": 1,
            "Geothermal Capacity": 3,
            "Oil - Spot crude prices": 3,
            "Solar Capacity": 3,
            "Wind Capacity": 3,
            "Cobalt and Lithium - Prices": 4
        }
        
        #sheets with custom index column
        self.names_custom_index = {
            "Gas - Proved reserves": "Trillion cubic metres", 
            "Oil - Proved reserves": "Thousand million barrels" #3
        }
        
        self.multiple_variables = {
         "Coal - Reserves": ["Coal - Reserves - Anthracite and bituminous", 
                     "Coal - Reserves - Sub-bituminous and lignite",
                     "Coal - Reserves - Total"],
        "Cobalt and Lithium - Prices": ["Cobalt and Lithium - Prices - Cobalt",
                                       "Cobalt and Lithium - Prices - Lithium Carbonate"],
        "Cobalt Production-Reserves": ["Cobalt Production-Reserves - Production",
                                      "Cobalt Production-Reserves - Reserves"],
        "Elec Gen by fuel": ["Elec Gen by fuel - Oil", "Elec Gen by fuel - Natural Gas",
                            "Elec Gen by fuel - Coal", "Elec Gen by fuel - Nuclear energy",
                            "Elec Gen by fuel - Hydro electric", "Elec Gen by fuel - Renewables",
                            "Elec Gen by fuel - Other #", "Elec Gen by fuel - Total"],
        "Primary Energy - Cons by fuel": ["Primary Energy - Cons by fuel - Oil", "Primary Energy - Cons by fuel - Natural Gas",
                            "Primary Energy - Cons by fuel - Coal", "Primary Energy - Cons by fuel - Nuclear energy",
                            "Primary Energy - Cons by fuel - Hydro electric", "Primary Energy - Cons by fuel - Renewables", 
                            "Primary Energy - Cons by fuel - Total"],
        "Renewables Generation by source": ["Renewables Generation by source - Wind", "Renewables Generation by source - Solar",
                                           "Renewables Generation by source - Other renewables+", "Renewables Generation by source - Total"]
        }
        

    # if custom is True then we use names_custom_index dict 
    def process_sheet(self, sh, skiprows, custom=False):

        data = pd.read_excel(self.data_path, na_values=['n/a'], 
              sheet_name=sh, 
              skiprows=skiprows)
        unit = "Total proved reserves" if custom else data.columns[0]
        data.fillna("none", inplace=True)
        try:
            startLoc = data[data[unit].str.contains(('Notes:|Note:'), na=False)].index.values[0]
            note = " ".join(data.loc[startLoc:][unit].values)
        except:
            note = ""

        unit_to_add = self.names_custom_index[sh] if custom else unit
        
        if sh in self.multiple_variables:
            for x in self.multiple_variables[sh]:
                
                self.names.append(x)
                self.units.append(unit_to_add)
                self.notes.append(note)
                self.ids.append(self.counter)
                self.counter += 1
        else:
            

            self.names.append(sh)
            self.units.append(unit_to_add)
            self.notes.append(note)
            self.ids.append(self.counter)
            self.counter += 1
            
    def run_all(self):
        for sh in tqdm(self.sheets):
            if sh in self.names_custom_start_row:
                self.process_sheet(sh, self.names_custom_start_row[sh], custom=False)
            elif sh in self.names_custom_index:
                self.process_sheet(sh, 1, custom=True)
            else:
                self.process_sheet(sh, 2, custom=False)
        
            


# In[12]:


dat = Data('./input/bp_stats.xlsx')
dat.run_all()


# In[13]:


final = pd.DataFrame({
    'id': dat.ids,
    'name': dat.names,
    'unit': dat.units,
    'notes': dat.notes
})


# In[14]:


final


# In[15]:


final.to_csv("./output/variables.csv", index=False)


# In[ ]:




