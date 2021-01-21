import urllib.request
import json

baseUrl='http://router.project-osrm.org/route/v1/driving/'

class Routing():
    def __init__(self,startPoint,endPoint):
        self.startPoint=startPoint
        self.endPoint=endPoint

    def saveRoutToDatabase(self,route,waypoints):
        query='''INSERT INTO public."Routes"(
	"startPoint", "endPoint", distance, duration, route)
	VALUES (%s,%s,%s,%s, ST_LineFromEncodedPolyline('''+chr(39)+route[0].get('geometry')+chr(39)+''') )
        returning "Routes"."ID";'''
        startPoint='POINT('+str(self.startPoint[1])+' '+str(self.startPoint[0])+')'
        endPoint='POINT('+str(self.endPoint[1])+' '+str(self.endPoint[1])+')'
        values=(startPoint,endPoint,route[0].get('distance'),route[0].get('duration'))
        return query,values

    def updateRout(self,route,waypoints,routeId):
        query='''UPDATE public."Routes"
	SET route=ST_LineFromEncodedPolyline('''+chr(39)+route[0].get('geometry')+chr(39)+''')
	WHERE "ID"='''+str(routeId)
        return query
        
    def Route(self):
        url=baseUrl+str(self.startPoint[1])+','+str(self.startPoint[0])+';'+str(self.endPoint[1])+','+str(self.endPoint[0])
        url=url+'?overview=simplified'
        #print(url)
        try:
            response = urllib.request.urlopen(url)
        except:
            return None
        string = response.read().decode('utf-8')
        jsonObj = json.loads(string)
        route=jsonObj.get('routes')
        waypoints=jsonObj.get('waypoints')
        result=self.saveRoutToDatabase(route,waypoints)
        return result

    def RouteByPoints(self,pointList):
        url=baseUrl
        for point in pointList:
            url=url+str(point[0])+','+str(point[1])+';'
        url=url[:-1]
        url=url+'?overview=simplified'
        print(url)
        response = urllib.request.urlopen(url)
        string = response.read().decode('utf-8')
        jsonObj = json.loads(string)
        route=jsonObj.get('routes')
        waypoints=jsonObj.get('waypoints')
        result=self.saveRoutToDatabase(route,waypoints)
        return result

    
