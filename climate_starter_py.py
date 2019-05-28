#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from datetime import datetime
from datetime import timedelta
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import Session
import sys
sys.path.append("Advanced SQL -surfs UP")
# # Reflect Tables into SQLAlchemy ORM
engine = create_engine("sqlite:///hawaii.sqlite")



# In[4]:

result = {}
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[5]:


# We can view all of the classes that automap found
Base.classes.keys()

# In[6]:

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station


# In[7]:


# Create our session (link) from Python to the DB
session = Session(engine)


# # Exploratory Climate Analysis

# In[8]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
max_date = session.query(func.max(measurement.date)).one()
max_date= datetime.strptime(max_date[0], '%Y-%m-%d')

# Calculate the date 1 year ago from the last data point in the database
# Perform a query to retrieve the data and precipitation scores
m_Data = session.query(measurement.date, func.sum(measurement.prcp).label('precipitation')).filter(measurement.date >= (max_date - timedelta(days=365))).filter(measurement.prcp >0).group_by(measurement.date).all()

# Save the query results as a Pandas DataFrame and set the index to the date column
df_measurement = pd.DataFrame(m_Data)
df_measurement = df_measurement.set_index("date")
# Sort the dataframe by date
df_measurement = df_measurement.sort_values(by=["date"])
# Use Pandas Plotting with Matplotlib to plot the data
df_measurement = df_measurement.reset_index()

result["precipitation"] = df_measurement 

# Use Pandas to calcualte the summary statistics for the precipitation data
measure_full = session.query(measurement.id,measurement.station,measurement.date, measurement.prcp,measurement.tobs).all()
df_measure = pd.DataFrame(measure_full)
df_measure_describe = df_measure["prcp"].describe()

# Design a query to show how many stations are available in this dataset?
df_unique_station = df_measure["station"].nunique()

# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
df_station = df_measure[["station","id"]].groupby("station").count().sort_values(by="id", ascending=False).rename(columns={"id":"Total Count"})

result["stations"] = df_station
# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature most active station?
df_active_station = df_measure[["station","tobs"]].loc[df_measure["station"]==df_station.index[0]].describe()
df_active_station_mmm = df_active_station.loc[["min","max","mean"]]


# In[14]:


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram

max_date_temp = session.query(func.max(measurement.date)).filter(measurement.station==df_station.index[0]).one()
max_date_temp = datetime.strptime(max_date_temp[0], '%Y-%m-%d')

m_temp = session.query(measurement.tobs, func.count(measurement.tobs).label('frequency')).filter(measurement.date >= (max_date_temp - timedelta(days=365))).filter(measurement.station==df_station.index[0] ).group_by(measurement.tobs).all()
df_temp_data = pd.DataFrame(m_temp)
df_temp_data["tobs"] = df_temp_data["tobs"].astype("int64")

m_temp_tobs = session.query(measurement.date,func.max(measurement.tobs).label('tobs'))\
.filter(measurement.date >= (max_date_temp - timedelta(days=365))).group_by( measurement.date).all()
df_temp_tobs_data = pd.DataFrame(m_temp_tobs)
result["tobs"] = df_temp_tobs_data


# In[15]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temp_start(start_date):
    return session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start_date).all()
def calc_temps(start_date,end_date):
    return session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start_date).filter(measurement.date <= end_date).all()

# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
temp_data = calc_temp_start('2016-02-28')



