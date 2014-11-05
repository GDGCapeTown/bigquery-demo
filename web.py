import webapp2
import jinja2
import os
import json
import logging
import random
from math import pi, sin

import httplib2
from apiclient.discovery import build
from oauth2client.appengine import AppAssertionCredentials

SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_NUMBER = '283921890307'

credentials = AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery_service = build('bigquery', 'v2', http=http)

log = logging.getLogger(__name__)

k = 800.0

def get_table_from_result_list(result_list):
    """
    Convert a list of results into a list of lists with column names and rows.

    :param result_list:
    @return:
    """
    if not result_list:
        return []
    table = []
    column_names = []
    column_types = []
    number_of_columns = len(result_list[0]['schema']['fields'])
    for i in range(number_of_columns):
        column_names.append(result_list[0]['schema']['fields'][i]['name'])
        column_types.append(result_list[0]['schema']['fields'][i]['type'])

    table.append(column_names)

    for result in result_list:
        for row in result['rows']:
            row_to_add = []
            for i in range(number_of_columns):
                if column_types[i] == 'INTEGER':
                    row_to_add.append(int(row['f'][i]['v']))
                elif column_types[i] == 'FLOAT':
                    row_to_add.append(float(row['f'][i]['v']))
                else:       # defaults to STRING
                    row_to_add.append(row['f'][i]['v'])
            table.append(row_to_add)

    return table

def get_location_of_subscribers_query(scalingfactor, south, north, west, east, hourofday):
    
    # Bottom left corner must always be the min longitude, and top right longitude must always be the max
    # else the query inequality wont work
    # This fixes issue when the view wraps across international date line
    
    if east >= west:
        # the normal case

        query = """
        SELECT
          lat_scaled/{scalingfactor} AS lat,
          lon_scaled/{scalingfactor} AS lon,
          count
        FROM 
          (SELECT 
            ROUND(lat*{scalingfactor}, 0) AS lat_scaled,
            ROUND(lon*{scalingfactor}, 0) AS lon_scaled,
            COUNT(*) AS count
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lon >  {west} AND
            lat < {north} AND
            lon <  {east} AND
            HOUR(measured) = {hourofday}
           GROUP BY lat_scaled, lon_scaled
          )
        ORDER BY count DESC 
        LIMIT 10000;
        """.format(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east, hourofday=hourofday)

    else:
        # east < west because wrapped around date line

        query = """
        SELECT
          lat_scaled/{scalingfactor} AS lat,
          lon_scaled/{scalingfactor} AS lon,
          count
        FROM 
          (SELECT 
            ROUND(lat*{scalingfactor}, 0) AS lat_scaled,
            ROUND(lon*{scalingfactor}, 0) AS lon_scaled,
            COUNT(*) AS count
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lat < {north} AND
            (lon < {east} OR lon > {west}) AND
            HOUR(measured) = {hourofday}
           GROUP BY lat_scaled, lon_scaled
          )
        ORDER BY count DESC 
        LIMIT 10000;
        """.format(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east, hourofday=hourofday)

    return query

def get_subscribers_on_basestations_query(south, north, west, east, hourofday):
    if east >= west:
        query = """
        SELECT
          cell_towers.lat AS lat,
          cell_towers.lon AS lon,
          measurements.count AS count
        FROM africacom.cell_towers AS cell_towers
        INNER JOIN 
        (
           SELECT
            mcc,
            net,
            area,
            cell,
            COUNT(*) AS count
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lon >  {west} AND
            lat < {north} AND
            lon <  {east} AND
            HOUR(measured) = {hourofday}
           GROUP BY mcc, net, area, cell
        ) AS measurements
        ON 
          cell_towers.mcc = measurements.mcc AND
          cell_towers.net = measurements.net AND
          cell_towers.area = measurements.area AND
          cell_towers.cell = measurements.cell
        ORDER BY measurements.count DESC
        LIMIT 10000;
        """.format(south=south, west=west, north=north, east=east, hourofday=hourofday)
    else:        
        query = """
        SELECT
          cell_towers.lat AS lat,
          cell_towers.lon AS lon,
          measurements.count AS count
        FROM africacom.cell_towers AS cell_towers
        INNER JOIN 
        (
           SELECT
            mcc,
            net,
            area,
            cell,
            COUNT(*) AS count
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lat < {north} AND
            (lon < {east} OR lon > {west}) AND
            HOUR(measured) = {hourofday}
           GROUP BY mcc, net, area, cell
        ) AS measurements
        ON 
          cell_towers.mcc = measurements.mcc AND
          cell_towers.net = measurements.net AND
          cell_towers.area = measurements.area AND
          cell_towers.cell = measurements.cell
        ORDER BY measurements.count DESC
        LIMIT 10000;
        """.format(south=south, west=west, north=north, east=east, hourofday=hourofday)
    return query

def get_signal_strength_query(scalingfactor, south, north, west, east):
    if east >= west:
        query = """
        SELECT
          lat_scaled/{scalingfactor} AS lat,
          lon_scaled/{scalingfactor} AS lon,
          ave_signal_strength AS count,
          num_measurements
        FROM
          (SELECT
            ROUND(lat*{scalingfactor}, 0) AS lat_scaled,
            ROUND(lon*{scalingfactor}, 0) AS lon_scaled,
            AVG(signal) AS ave_signal_strength,
            COUNT(*) AS num_measurements
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lon >  {west} AND
            lat < {north} AND
            lon <  {east} AND
            signal > 0 AND
            signal <= 32
           GROUP BY lat_scaled, lon_scaled
          )
        ORDER BY ave_signal_strength ASC
        LIMIT 10000;
        """.format(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east)
    else:
        query = """
        SELECT
          lat_scaled/{scalingfactor} AS lat,
          lon_scaled/{scalingfactor} AS lon,
          ave_signal_strength AS count,
          num_measurements
        FROM
          (SELECT
            ROUND(lat*{scalingfactor}, 0) AS lat_scaled,
            ROUND(lon*{scalingfactor}, 0) AS lon_scaled,
            AVG(signal) AS ave_signal_strength,
            COUNT(*) AS num_measurements
           FROM [africacom.measurements_10]
           #FROM (TABLE_QUERY(africacom, 'table_id CONTAINS "measurements"'))
           WHERE
            lat > {south} AND
            lat < {north} AND
            (lon < {east} OR lon > {west}) AND
            signal > 0 AND
            signal <= 32
           GROUP BY lat_scaled, lon_scaled
          )
        ORDER BY ave_signal_strength DESC
        LIMIT 10000;
        """.format(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east)
    return query

def insert_query(query_string):
    """
    Run an asynchronous query on BigQuery

    :param query_string: The BigQuery query string
    """
    from apiclient.errors import HttpError

    #TODO: Dry run first to see if the query is valid?

    global bigquery_service
    global PROJECT_NUMBER

    response_list = []
    bigquery = bigquery_service
    body = {'query': query_string, 'timeoutMs': 0}
    try:
        # Source: https://developers.google.com/bigquery/querying-data#asyncqueries
        job_collection = bigquery.jobs()
        project_id = PROJECT_NUMBER
        insert_response = job_collection.query(
            projectId=project_id,
            body=body).execute()

        jobReference=insert_response['jobReference']
        while(not insert_response['jobComplete']):
            print 'Job not yet complete...'
            insert_response = job_collection.getQueryResults(
                projectId=jobReference['projectId'],
                jobId=jobReference['jobId'],
                timeoutMs=0).execute()
        
        current_row = 0
        while u'rows' in insert_response and current_row < insert_response[u'totalRows']:
            response_list.append(insert_response)
            current_row += len(insert_response[u'rows'])
            insert_response = job_collection.getQueryResults(
                projectId=project_id,
                jobId=insert_response[u'jobReference'][u'jobId'],
                startIndex=current_row).execute()

        return get_table_from_result_list(response_list)
    except HttpError:
        log.exception(u'Failed to run query [%s] in BigQuery', query_string)
        return None

def table_to_latloncount(table):
    response = []

    for row in table[1:]:
        measuremment = {}
        measuremment['lat'] = row[0]
        measuremment['lon'] = row[1]
        measuremment['count'] = row[2]

        response.append(measuremment)

    return response

def getFrameForQuery(query):
    response = insert_query(query)
    response = table_to_latloncount(response)

    response = {
        'data': response
    }

    return response


JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

class LocationOfSubscribers(webapp2.RequestHandler):
    def get(self):
        template_values = {}

        template = JINJA_ENVIRONMENT.get_template('/views/locationofsubscribers.html')
        self.response.write(template.render(template_values))

class SubscribersOnBasestations(webapp2.RequestHandler):
    def get(self):
        template_values = {}

        template = JINJA_ENVIRONMENT.get_template('/views/subscribersonbasestations.html')
        self.response.write(template.render(template_values))

class SignalStrength(webapp2.RequestHandler):
    def get(self):
        template_values = {}

        template = JINJA_ENVIRONMENT.get_template('/views/signalstrength.html')
        self.response.write(template.render(template_values))

class LocationOfSubscribersFrame(webapp2.RequestHandler):
    def get(self):
        seconds_since_midnight = int(self.request.get('seconds_since_midnight'))
        hourofday = seconds_since_midnight/3600
        north = float(self.request.get('neLat'))
        east = float(self.request.get('neLon'))
        south = float(self.request.get('swLat'))
        west = float(self.request.get('swLon'))

        if east >= west:
            delta_lon = east - west
        else:
            delta_lon = (-180 - east) + (180 - west)

        global k
        scalingfactor = k/delta_lon

        query = get_location_of_subscribers_query(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east, hourofday=hourofday)

        response = getFrameForQuery(query=query)
        response['seconds_since_midnight'] = seconds_since_midnight

        self.response.write(json.dumps(response))

class SubscribersOnBasestationsFrame(webapp2.RequestHandler):
    def get(self):
        seconds_since_midnight = int(self.request.get('seconds_since_midnight'))
        hourofday = seconds_since_midnight/3600
        north = float(self.request.get('neLat'))
        east = float(self.request.get('neLon'))
        south = float(self.request.get('swLat'))
        west = float(self.request.get('swLon'))

        query = get_subscribers_on_basestations_query(south=south, west=west, north=north, east=east, hourofday=hourofday)

        response = getFrameForQuery(query=query)
        response['seconds_since_midnight'] = seconds_since_midnight

        self.response.write(json.dumps(response))

class SignalStrengthFrame(webapp2.RequestHandler):
    def get(self):
        seconds_since_midnight = int(self.request.get('seconds_since_midnight'))
        hourofday = seconds_since_midnight/3600
        north = float(self.request.get('neLat'))
        east = float(self.request.get('neLon'))
        south = float(self.request.get('swLat'))
        west = float(self.request.get('swLon'))

        if east >= west:
            delta_lon = east - west
        else:
            delta_lon = (-180 - east) + (180 - west)

        global k
        scalingfactor = k/delta_lon

        query = get_signal_strength_query(scalingfactor=scalingfactor, south=south, west=west, north=north, east=east)

        response = getFrameForQuery(query=query)
        response['seconds_since_midnight'] = seconds_since_midnight

        self.response.write(json.dumps(response))


app = webapp2.WSGIApplication([
    ('/', LocationOfSubscribers),
    ('/locationofsubscribers', LocationOfSubscribers),
    ('/subscribersonbasestations', SubscribersOnBasestations),
    ('/signalstrength', SignalStrength),
    ('/getFrame/locationofsubscribers', LocationOfSubscribersFrame),
    ('/getFrame/subscribersonbasestations', SubscribersOnBasestationsFrame),
    ('/getFrame/signalstrength', SignalStrengthFrame)
], debug=True)
