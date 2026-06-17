import base64
import json
import os
import sys
import urllib.request

username = os.environ.get('NEXUS_USERNAME')
password = os.environ.get('NEXUS_PASSWORD')

def get(url, username, password):
    req = urllib.request.Request(url)
    base64_auth = base64.b64encode(bytes('{}:{}'.format(username, password),'ascii'))
    req.add_header("Authorization", "Basic {}".format(base64_auth.decode('utf-8')))
    req.add_header('Accept', 'application/json')
    with urllib.request.urlopen(req) as response:
        return response.read()

def getRepositories(username, password):
    return json.loads(get("https://oss.sonatype.org/service/local/staging/profile_repositories", username, password))

repositories = getRepositories(username, password).get("data")
if len(repositories) != 1:
    sys.stderr.write("Zero or more than one staging repository. Exiting. Please execute the process manually.")
    exit(1)

repositoryId = repositories[0].get("repositoryId")
print(repositoryId)


