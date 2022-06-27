# ICRI Envirosensor Data Analysis

This repository contains a basic example for reading and analysing the original ICRI Envirosensor data using Apache Spark in Python.

The project data is also avialble for download under Creative Commons license with attribution from the IEEE data portal: https://ieee-dataport.org/open-access/icri-envirosensor-data

## ABSTRACT 

The Here East Digital Twin was a six month trial of a real-time 3D data visualisation platform, designed for the purpose of supporting operational management in the built environment.

The trial used eighteen custom sensing devices created by the Intel Collaborative Research Institute (ICRI) for Urban IoT. Data from the sensors were aggregated in a cloud-based platform which then transmitted the data to specially designed client applications which visualised the data in the context of a real-time 3D model or 'Digital Twin'. The Digital Twin was accessible to site occupants and visitors in situ using WebGL. In this way a shared view of site operation was made available to building managers, occupants and visitors.

This data collection provides 22 weeks of raw sensor readings captured at a frequency of one reading (per sensor) every sixty seconds from Ti CC2650STK SensorTags. Readings include:

- Ambient Light (OPT / OPT3001)
- Ambient Temperature (TMP / TMP007)
- Temperature (BAT / BMP280)
- Ambient Temperature (HDT / HDC1000)
- Barometric Pressure (BAR / BMP280)
- Humidity (HDH / HDC1000)

The data are made available for experimentation in data visualisation, data modelling, simulation, IoT data pipeline development or machine learning.

NOTE: Data were captured under operational site conditions as part of an 'in the wild' deployment. Data are provided as read from the sensor. Sensors may have been moved or tampered with while onsite. They may also have suffered faults or experienced downtime.

Thank you to the Intel Collaborative Research Institute (ICRI) for Urban IoT for providing the Envirosensor platform. 

This project is linked to research at CASA funded by the Engineering and Physical Science Research Council (EPSRC) and Ordnance Survey (OS), the national mapping agency for Great Britain.

## Instructions: 
1. View the project video for context: https://vimeo.com/311089492

2. Review details of the Envirosensor implementation: https://github.com/virtualarchitectures/ICRI_Envirosensor

3. One option for analysing the data is using Spark and Python in a Jupyter Notebook as outlined in the following example GitHub repository: https://github.com/virtualarchitectures/ICRI_Envirosensor_Data_Analysis
