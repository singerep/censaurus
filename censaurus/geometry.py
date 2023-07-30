from typing import Union
from io import BytesIO
import geopandas as gpd
import ftplib
import requests
import shapely

from censaurus.api import TIGERClient

url = 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2010/MapServer/94/query?where=GEOID%3D%2712420%27&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=5&outSR=4236&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=json'
resp = requests.get(url)
rings = resp.json()['features'][0]['geometry']['rings'][0]

p = shapely.Polygon(rings)


import matplotlib.pyplot as plt

x,y = p.exterior.xy
plt.plot(x,y)
plt.show()