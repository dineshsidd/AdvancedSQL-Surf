from flask import Flask, jsonify
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import Session
import pandas as pd
import json
import sys
sys.path.append("Advanced SQL -surfs UP")
from climate_starter_py import result
from climate_starter_py import calc_temps
df_precipitation = pd.DataFrame(result["precipitation"]).set_index("date")
precip_json = df_precipitation.to_json(orient="columns")



app = Flask(__name__)

@app.route("/")
def welcome():
  return  (
        f"Welcome to the Climate App!<br/>"
        f"-------------------------------<br/>"
        f"Available Routes:<br/>"
        f"<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start-date    (Format : yyyy-mm-dd)<br/>"
        f"/api/v1.0/start-date/end-date   (Format : yyyy-mm-dd)"

    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    df_precipitation = pd.DataFrame(result["precipitation"]).set_index("date")
    precip_json = df_precipitation.to_json(orient="columns")
    precip_html = df_precipitation.to_html()
    return (
         f"<h3> Precipitation Data in both HTML Table & JSON formats( Scroll Down) </h3>"
         f"{precip_html} <br/>"
         f"<br/>"
         f"{precip_json} <br/>"
    )

@app.route("/api/v1.0/stations")
def stations():
    df_stations = pd.DataFrame(result["stations"])
    station_json = df_stations.to_json(orient="columns")
    station_html = df_stations.to_html()
    return (
         f"<h3> Stations Data in both HTML Table & JSON formats </h3>"
         f"{station_html} <br/>"
         f"<br/>"
         f"{station_json} <br/>"
    )

@app.route("/api/v1.0/tobs")
def tobs():
    df_tobs = pd.DataFrame(result["tobs"]).set_index("date")
    tobs_json = df_tobs.to_json(orient="columns")
    tobs_html = df_tobs.to_html()
    return (
         f"<h3> Temprature Observations in HTML Table & JSON format (Scroll Down) </h3>"
         f"{tobs_html} <br/>"
         f"<br/>"
         f"{tobs_json} <br/>"
    )

@app.route("/api/v1.0/<start>")
def tempStart(start):
    # # Reflect Tables into SQLAlchemy ORM
    engine = create_engine("sqlite:///hawaii.sqlite")
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    measurement = Base.classes.measurement
    session = Session(engine)
    st_out = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start).all()
    return (
        f"<h3>Min , Avg & Max Tempratures since : {start}</h3><br/>"
        f"Min : {str(st_out[0][0])} <br/>"
        f"Avg : {str(st_out[0][1])} <br/>"
        f"Max : {str(st_out[0][2])} <br/>" )

@app.route("/api/v1.0/<start>/<end>")
def temp(start,end):
    # # Reflect Tables into SQLAlchemy ORM
    engine = create_engine("sqlite:///hawaii.sqlite")
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    measurement = Base.classes.measurement
    session = Session(engine)
    st_out_all = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start).filter(measurement.date <= end).all()
    return (
        f"<h3>Min , Avg & Max Tempratures from : {start} till : {end} </h3><br/>"
        f"Min : {str(st_out_all[0][0])} <br/>"
        f"Avg : {str(st_out_all[0][1])} <br/>"
        f"Max : {str(st_out_all[0][2])} <br/>" )

if __name__ == "__main__" :
    app.run(debug=True)
