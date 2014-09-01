import httplib, urllib

DBNAME = "medusa"
DB_ACCOUNT = (open('./config/db_account.info').read().rstrip()).split('|')
DB_USERNAME = DB_ACCOUNT[0]
DB_PASSWORD = DB_ACCOUNT[1]
DB_HOST = open('./config/db_host.info').read().rstrip()

#Form parameters for POST request
SPARQL_ENDPOINT = "SparqlEndPoint"
GRAPH_URI = "GraphURI"
R2RML_URL = "R2rmlURI"
DATA_URL = "DataURL"
RAW_DATA = "RawData"
USERNAME = "UserName"
PASSWORD = "Password"
TRIPLE_STORE = "TripleStore"
TRIPLE_STORE_SESAME = "Sesame"
TRIPLE_STORE_VIRTUOSO = "Virtuoso"
OVERWRITE = "Overwrite"
CONTENT_TYPE = "ContentType"
CONTENT_TYPE_CSV = "CSV"
CONTENT_TYPE_JSON = "JSON"
CONTENT_TYPE_XML = "XML"
REFRESH_MODEL = "RefreshModel"

#Form parameters value - SHOULD BE READ FROM A CONFIG FILE LATER
SPARQL_ENDPOINT_VALUE = "http://fusion-sqid.isi.edu:8890/sparql-graph-crud-auth/"
GRAPH_URI_VALUE = "http://fusion-sqid.isi.edu:8890/image-metadata" #could be any URI
R2RML_URL_VALUE = "http://fusion-sqid.isi.edu/WSP1WS2-metadata.json-model.ttl"
DATA_URL_VALUE = "DataURL"
USERNAME_VALUE = "finimg"
PASSWORD_VALUE = "isi"
OVERWRITE_VALUE = "True"
REFRESH_MODEL_VALUE = "True"
RAW_DATA_VALUE = "{\"metadata\":{\"GPSTimeStamp\":\"NOT_AVAILABLE\",\"ISOSpeedRatings\":\"100\",\"Orientation\":\"6\",\"Model\":\"GT-N7100\",\"WhiteBalance\":\"0\",\"GPSLongitude\":\"NOT_AVAILABLE\",\"ImageLength\":\"2448\",\"FocalLength\":\"3.7\",\"HasFaces\":\"1\",\"ImageName\":\"20140707_134558.jpg\",\"GPSDateStamp\":\"NOT_AVAILABLE\",\"Flash\":\"0\",\"DateTime\":\"2014:07:07 13:45:58\",\"NumberOfFaces\":\"1\",\"ExposureTime\":\"0.020\",\"GPSProcessingMethod\":\"NOT_AVAILABLE\",\"FNumber\":\"2.6\",\"ImageWidth\":\"3264\",\"GPSLatitude\":\"NOT_AVAILABLE\",\"GPSAltitudeRef\":\"-1\",\"Make\":\"SAMSUNG\",\"GPSAltitude\":\"-1.0\"}}"



def publish_rdf():
    query = ("select * from Meta_data")
    print query
    con, cur = sql_execute()
    cur.execute(query)
    numrows = int(cur.rowcount)
    for i in range(0,numrows):
        row = cur.fetchone()
        print row[3]
        call_webservice(row[3])


def call_webservice(rawdata):
        params = urllib.urlencode({SPARQL_ENDPOINT : SPARQL_ENDPOINT_VALUE, GRAPH_URI : GRAPH_URI_VALUE,
                                   TRIPLE_STORE : TRIPLE_STORE_VIRTUOSO, OVERWRITE : OVERWRITE_VALUE , REFRESH_MODEL : REFRESH_MODEL_VALUE,
                                   CONTENT_TYPE : CONTENT_TYPE_JSON, RAW_DATA : RAW_DATA_VALUE, R2RML_URL : R2RML_URL_VALUE, USERNAME : USERNAME_VALUE, PASSWORD : PASSWORD_VALUE})
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        conn = httplib.HTTPConnection("fusion-sqid.isi.edu","8080")
        conn.request("POST", "/publishrdf/rdf/r2rml/rdf/sparql", params, headers)
        response = conn.getresponse()
        print response.status, response.reason
        data = response.read()
        print data
        conn.close()



def sql_execute():
	import MySQLdb as mdb
	con = None
	data = None
	try:
		con = mdb.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DBNAME)
		cur = con.cursor()
	except mdb.Error, e:
		print e
	return con, cur

if __name__ == '__main__':
	publish_rdf()