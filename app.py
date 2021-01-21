from flask import Flask, request
from flask import jsonify
import psycopg2
import json
import maps
import DataBaseProcessing
import time
import getWeather
import datetime


key='key_to_darksky_api'
dbname='PostrgeSQL+PostGis'
user='username'
psw='password'

app=Flask(__name__)

def makePointsByRout(routId):
    conn = psycopg2.connect("dbname=  user=  password= ")
    cur = conn.cursor()
    stasment=''' insert into "RoutPoints"
        SELECT "Routes"."ID",	
        (ST_DumpPoints("Routes"."route")).geom,
		(ST_DumpPoints("Routes"."route")).path[1],
		'intemediate'
        FROM "Routes"
        where	"Routes"."ID"='''+str(routId)
    cur.execute(stasment)
    conn.commit()
    stasment='''select count(*) from "RoutPoints" where "RoutPoints"."RoutID"='''+str(routId)
    cur.execute(stasment)
    result=cur.fetchall()
    conn.close()
    return result[0][0]

def setNewPoint(routId,rad,order,first,radMin):
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor()
    print(order)
    stasment='''SELECT
                    "RoutPoints"."order",
                    st_setsrid("RoutPoints"."wayPoint",4326) nowPoint,
                    ST_Length(ST_LineSubstring("Routes"."route",
                                               ST_LineLocatePoint("Routes"."route", st_setsrid((select "RoutPoints"."wayPoint" from "RoutPoints" where
                                               "RoutPoints"."RoutID"='''+str(routId)+''' and "RoutPoints"."order"='''+str(order)+'''),4326)),
                                               ST_LineLocatePoint("Routes"."route", st_setsrid("RoutPoints"."wayPoint",4326))
                                                                           )::geography) length
                from "Routes"
                join "RoutPoints" on "Routes"."ID"="RoutPoints"."RoutID"
                where "Routes"."ID"='''+str(routId)+''' and "RoutPoints"."order">'''+('1' if first else str(order))+''' 
                and ST_Length(ST_LineSubstring("Routes"."route",
                                               ST_LineLocatePoint("Routes"."route", st_setsrid((select "RoutPoints"."wayPoint" from "RoutPoints" where
                                               "RoutPoints"."RoutID"='''+str(routId)+''' and "RoutPoints"."order"='''+str(order)+'''),4326)),
                                               ST_LineLocatePoint("Routes"."route", st_setsrid("RoutPoints"."wayPoint",4326))
                                                                           )::geography) <='''+str(rad)+'''
                order by 3 desc'''
    #print(stasment)
    try:
        cur.execute(stasment)
        result=cur.fetchall()
    except:
        return None
    #print(result)
    if result==None or len(result)==0:
        return None
    maxPoint=result[0][0]
    stasment='''	SELECT "Points"."ID",
				st_setsrid("Points"."Geom",4326),
				st_setsrid("RoutPoints"."wayPoint",4326),
				sT_Distance(st_setsrid("Points"."Geom",4326), st_setsrid("RoutPoints"."wayPoint",4326))
	FROM "Points"
		LEFT JOIN "RoutPoints" ON ST_DWithin(st_setsrid("Points"."Geom",4326), st_setsrid("RoutPoints"."wayPoint",4326), 75)
		and "RoutPoints"."order"='''+str(maxPoint)+'''
        where "Points"."HasCharger"
	ORDER BY ST_Distance(st_setsrid("Points"."Geom",4326), st_setsrid("RoutPoints"."wayPoint",4326))'''
    cur.execute(stasment)
    result=cur.fetchall()
    neededCharger=result[0][0]
    stasment='''update "RoutPoints"
                set "wayPoint"=(select st_setsrid("Points"."Geom",4326) from "Points" where "Points"."ID"='''+str(neededCharger)+'''),
                "type"='sleep'
                where "RoutPoints"."order"='''+str(maxPoint)
    cur.execute(stasment)
    conn.commit()
    conn.close()
    return maxPoint
    

def addPointToWay(point,routId,order,type_):
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor()
    stasment='''INSERT INTO public."RoutPoints"(
	"RoutID", "wayPoint", "order","type")
	VALUES ('''+str(routId)+''',ST_GeomFromText('POINT('''+str(point[0])+''' '''+str(point[1])+''')'),'''+str(order)+''' ,
        '''+chr(39)+type_+chr(39)+''' );'''
    cur.execute(stasment)
    conn.commit()
    conn.close()

    
def getUserCar(userId,routId):
    stasment='''select CASE WHEN (round("Car"."One_charge_distance"*"Wether_Trable"."distanse_slow")::int) IS NULL 
THEN "Car"."One_charge_distance" 
ELSE (round("Car"."One_charge_distance"*"Wether_Trable"."distanse_slow")::int) 
END as One_charge_distance from "Customers" inner join "Customer_Cars" 
on "Customers"."Id" = "Customer_Cars"."Customer_id" inner join "Car" 
on "Customer_Cars"."Car_id" = "Car"."Id" 
left join "Wether_Trable" on "Wether_Trable"."Id" = (select max("Id") from "Wether_Trable" where "Wether_Trable"."temperature" >= 
(select avg("Weather"."temperature") from "Weather" 
join "WeatherForecasts" on "Weather"."weatherForecastsId"="WeatherForecasts"."id" 
                where "WeatherForecasts"."routId"='''+str(routId)+''')) 
                where "Customers"."User_id" = '''+str(userId)


##    '''select  "Car"."One_charge_distance"   from  "Customers" inner join "Customer_Cars" 
##        on "Customers"."Id" = "Customer_Cars"."Customer_id" inner join "Car" 
##        on "Customer_Cars"."Car_id" = "Car"."Id"
##        where "Customers"."User_id" ='''+str(userId)
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor() 
    cur.execute(stasment)
    result=cur.fetchall()
    conn.close()
    return result[0][0]


def getPointList(routId):
    stasment='''SELECT st_astext("wayPoint"),"type"
                FROM "RoutPoints"
                where "RoutID"='''+str(routId)+'''
                and ("type"='start' or "type"='finish' or "type"='sleep')
                order by "order"'''
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor()
    cur.execute(stasment)
    result=cur.fetchall()
    conn.close()
    points=[]
    for point in result:
        strPoi=point[0]
        strPoi=strPoi.replace('POINT(','')
        strPoi=strPoi.replace(')','')
        strPoiList=strPoi.split(' ')
        strPoiList.append(point[1])
        points.append(strPoiList)
    return points

def insertMainWeatherData(coordinates,timezone,offset,date,time,routId,conn):
    query='''INSERT INTO public."WeatherForecasts"(
	            "сoordinates", "timezone", "offset","date","time","routId")
	     VALUES (%s,%s,%s,%s,%s,%s)
            returning id;'''
    values=(coordinates,timezone,offset,date,time,routId)
    return conn.insert(query,values,True)

def insertWeatherData(wethId,forecasts,conn):
    query='''INSERT INTO public."Weather"(
	"weatherForecastsId", date, "time", summary, icon, "precipIntensity",
	"precipProbability", "precipType", temperature, "apparentTemperature",
	"dewPoint", humidity, pressure, "windSpeed", "windGust", "windBearing",
	"cloudCover", "uvIndex", visibility, ozone)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    for forecast in forecasts:
        timestamp=forecast.get('time')
        value=datetime.datetime.fromtimestamp(timestamp)
        date=value.strftime('%Y-%m-%d')
        time=value.strftime('%H:%M:%S')
        values=(wethId,date,time,forecast.get('summary'),forecast.get('icon'),forecast.get('precipIntensity'),
                forecast.get('precipProbability'),forecast.get('precipType'),forecast.get('temperature'),
                forecast.get('apparentTemperature'),forecast.get('dewPoint'),forecast.get('humidity'),
                forecast.get('pressure'),forecast.get('windSpeed'),forecast.get('windGust'),
                forecast.get('windBearing'),forecast.get('cloudCover'),forecast.get('uvIndex'),
                forecast.get('visibility'),forecast.get('ozone'))
        conn.insert(query,values,False)

@app.errorhandler(404)
def page_not_found(e):
    return 'you have fucking error'

@app.route('/index')
def index():
    return 'hello' 

@app.route('/greenroad/api/v1/login', methods=['POST'])
def checkLogin():
    login=request.args.get('login')
    password=request.args.get('password')
    stasment='''SELECT "Users"."Id","Customers"."Id","Owners"."Id"
                FROM "Users"
                left join "Customers" on "Users"."Id"="Customers"."User_id"
                left join "Owners" on "Users"."Id"="Owners"."User_id"
                where "Users"."Login"='''+chr(39)+login+chr(39)+''' and "Users"."Password"='''+chr(39)+password+chr(39)
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor()
    cur.execute(stasment)
    result=cur.fetchall()
    conn.close()
    if result==None:
        return jsonify({'error':'Authorization error. Login or password not found.'})
    else:
        return jsonify({'userId':result[0][0],'isCustomer':result[0][1],'isOwner':result[0][2]})
        
@app.route('/greenroad/api/v1/OwnerInfo/<int:ownId>', methods=['Get']) 
def getUserStatistic(ownId): 
    conn = psycopg2.connect(dbname='', user='', 
    password='', host='' ) 
    cursor = conn.cursor() 
    cursor.execute('''select "Charger_Name",count(*),avg("Time_start"::time- "Time_finish"::time ) from "Charger_Info" 
    left join "Charge_Event" on "Charger_Info"."Id" = "Charge_Event"."Charger_id" and "Owner_id" = '''+str(ownId)+''' 
    where "Charge_Event"."Date_start" between (current_date - interval '5 month') and current_date group by "Charger_Name" 
    order by 1''') 
    _return = list() 
    for row in cursor: 
        _return.append({'Charger_Name':row[0],'Numbers':row[1],'Time':row[2].seconds//3600}) 
    cursor.close() 
    conn.close() 
    print(jsonify(_return)) 
    return jsonify(_return)

@app.route('/greenroad/api/v1/neighbor', methods=['GET'])
def getNeighbor():
    lat=request.args.get('lat')
    lon=request.args.get('lon')
    rad=int(request.args.get('rad'))
    stasment='''SELECT  st_astext("Geom"), "Points"."Type"
                FROM public."Points"
                WHERE ST_DWithin("Geom", ST_GeomFromText('POINT('''+lat+''' '''+lon+''')'), '''+str(rad)+''', TRUE)
                and "HasCharger"'''
    conn = psycopg2.connect("dbname= user= password=")
    cur = conn.cursor()
    cur.execute(stasment)
    result=cur.fetchall()
    conn.close()
    res=[]
    for point in result:
        strPoi=point[0]
        strPoi=strPoi.replace('POINT(','')
        strPoi=strPoi.replace(')','')
        strPoiList=strPoi.split(' ')
        strPoiDict={'lat':strPoiList[0],'lon':strPoiList[1],'type':point[1]}
        res.append(strPoiDict)
    return jsonify(res)

@app.route('/greenroad/api/v1/route', methods=['GET'])
def getRout():
    latBeg=request.args.get('latBeg')
    lonBeg=request.args.get('lonBeg')
    latEnd=request.args.get('latEnd')
    lonEnd=request.args.get('lonEnd')
    userId=request.args.get('userId')
    charged=int(request.args.get('charged'))
    router=maps.Routing([lonBeg,latBeg],[lonEnd,latEnd])
    mainRout=None
    while mainRout==None:
        print('Try request')
        mainRout=router.Route()
        time.sleep(2)
    conn=DataBaseProcessing.databaseConnection(dbname,user,psw)
    conn.connect()
    routId=conn.insert(mainRout[0],mainRout[1],True)
    weather=getWeather.Weather(key,lonBeg,latBeg)
    daily=weather.getWeather()
    now=datetime.datetime.now()
    coordinates='POINT('+str(daily.get('longitude'))+' '+str(daily.get('latitude'))+')'
    timezone=daily.get('timezone')
    offset=daily.get('offset')
    date=now.strftime('%Y-%m-%d')
    time_=now.strftime('%H:%M:%S')
    wethId=insertMainWeatherData(coordinates,timezone,offset,date,time_,routId,conn)
    insertWeatherData(wethId,daily.get('hourly').get('data'),conn)
    addPointToWay([latBeg,lonBeg],routId,0,'start')
    countPoints=makePointsByRout(routId)
    maxRad=getUserCar(userId,routId)
    rad=maxRad*(charged/100)*(80/100)
    radMin=maxRad*(charged/100)*(15/100)
    order=0
    order=setNewPoint(routId,rad,order,True,radMin)
    while order!=None:
        order=setNewPoint(routId,rad,order,False,radMin)
    addPointToWay([latEnd,lonEnd],routId,countPoints-1,'finish')     
    pointList=getPointList(routId)
    res=[]
    for point in pointList:
        strPoiDict={'lat':point[0],'lon':point[1],'type':point[2]}
        res.append(strPoiDict)
    del conn
    return jsonify(res)



    
if __name__=='__main__':
    app.run(host='0.0.0.0',debug=True)

