#!/usr/bin/env python
# coding: utf-8

# In[9]:


import os
import pandas as pd
from glob import glob


# In[10]:


countries = set()

for f in glob('./output/datapoints/*.csv'):
    df = pd.read_csv(f)
    countries |= set(df['country'].tolist())
        


# In[11]:


res = pd.DataFrame()
res['name'] = list(countries)


# In[12]:


res.to_csv("./standardization/entities.csv", index=False)


# In[ ]:




