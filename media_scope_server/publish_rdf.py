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

#Triple Store URl
SPARQL_ENDPOINT_VALUE = "http:"
#could be any URI
GRAPH_URI_VALUE = "http://" 
#URL to fetch the R2RML model
R2RML_URL_VALUE = "http://"
DATA_URL_VALUE = "http://"
USERNAME_VALUE = "<username>"
PASSWORD_VALUE = "<password>"
#Overwrite the existing data True or False
OVERWRITE_VALUE = 
#Fetch the updated model from the URL <True or False>
REFRESH_MODEL_VALUE = 



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
                                   CONTENT_TYPE : CONTENT_TYPE_JSON, DATA_URL : DATA_URL_VALUE, R2RML_URL : R2RML_URL_VALUE, USERNAME : USERNAME_VALUE, PASSWORD : PASSWORD_VALUE})
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        conn = httplib.HTTPConnection("fusion-sqid.isi.edu","8080") # URL and PORT for Triplestore
        conn.request("POST", "/publishrdf/rdf/r2rml/rdf/sparql", params, headers) #the path for the REST web service
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
