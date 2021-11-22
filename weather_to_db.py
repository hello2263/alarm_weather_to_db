
from urllib.request import urlopen, Request
from urllib.parse import urlencode, unquote, quote_plus
from pymongo import MongoClient
from pymongo.cursor import CursorType
from datetime import datetime
import json
import pandas as pd

def insert_item_one(mongo, data, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].insert_one(data).inserted_id
    return result

def insert_item_many(mongo, datas, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].insert_many(datas).inserted_ids
    return result

def update_local(): ## 완성 -> 엑셀에 있는 지역, 도시, x, y좌표를 db에 입력
    count = 0
    Location = '/home/ec2-user/db/'
    File = 'weather_local_xy.xlsx'
    data_pd = pd.read_excel('{}/{}'.format(Location, File),
                            header=None, index_col=None, names=None)
    try:
        for row in data_pd.loc():
            param = []
            for item in row:
                param.append(item)
            cursor.execute("INSERT INTO local(area, city, x, y) VALUES ('"+param[0]+"', '"+param[1]+
            "', '"+str(param[2])+"', '"+str(param[3])+"') ON DUPLICATE KEY UPDATE x='"+str(param[2])+"', y='"+
            str(param[3])+"';")
            db.commit()
            count +=1
            if count == 25:
                break
        print("local update")
    except:
        print("local update error")

def set_date(): # 날짜를 api에 맞게 설정해줌
    global today_time, today_date
    now = datetime.now()
    today_time = int(str(now.hour)+str(now.minute))
    today_day = now.day
    if today_time < 215:
        today_day -= 1
        today_time = '2330'
    elif today_time < 515:
        today_time = '0230'
    elif today_time < 815:
        today_time = '0530'
    elif today_time < 1115:
        today_time = '0830'
    elif today_time < 1415:
        today_time = '1130'
    elif today_time < 1715:
        today_time = '1430'
    elif today_time < 2015:
        today_time = '1730'
    elif today_time < 2315:
        today_time = '2030'
    else:
        today_time = '2330'   

    if now.month < 10:
        today_month = '0'+str(now.month)
    else:
        today_month = str(now.month)

    if today_day < 10:
        today_day = '0'+str(today_day)
    else:
        today_day = str(today_day)
    today_date = str(now.year)+today_month+today_day # 20210905
    return today_date + '-' + today_time # 20210905-1400

def get_data():     # 기상청에 API를 요청하여 데이터를 받음
    CallBackURL = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
    params = '?' + urlencode({
        quote_plus("serviceKey"): 'XIjRFoewvUDp4EDhRpATADoatwElkiQ%2F1J0tDooGjBTKStjRtuW3Zu89iE9cBsK%2Bz299IJwkbaE%2F%2F7SzcVo2yA%3D%3D',
        quote_plus("numOfRows"): '1000',
        quote_plus('pageNo'): '1',
        quote_plus('dataType'): 'JSON',
        quote_plus('base_date'): today_date,
        quote_plus('base_time'): today_time,
        quote_plus('nx'): x.pop(0),
        quote_plus('ny'): y.pop(0)
    })
    request = Request(CallBackURL + unquote(params))    # URL 데이터 파싱
    response_body = urlopen(request).read()    # API를 통해 데이터 GET
    data = json.loads(response_body)    # JSON으로 변환
    item_data = data['response']['body']['items']['item']
    return item_data

def send_data(local):   # local에 해당하는 기상정보를 db에 넣음
    count = 0
    weather_data = dict() 
    item_data = get_data()
    for item in item_data:
        weather_data['지역'] = str(local)
        weather_data['타임'] = item['fcstDate'] +'-' +item['fcstTime']
        if item['category'] =='TMP': # 기온체크
            weather_data['기온'] = item['fcstValue']
            count += 1
        if item['category'] == 'POP': # 상태체크
            weather_data['강수확률'] = item['fcstValue']
            count += 1
        if item['category'] == 'SKY': # 하늘체크
            weather_code = item['fcstValue']
            count += 1
            if weather_code == '1':
                weather_state = 'sunny'
            elif weather_code == '3':
                weather_state = 'cloudy'
            elif weather_code == '4':
                weather_state = 'gray'
            else:
                weather_state = 'none'
            weather_data['하늘'] = weather_state
        if count == 3:
            cursor.execute("INSERT INTO seoul(local, date, tmp, rain, sky) VALUES ('"+weather_data['지역']+"', '" +
                        weather_data['타임']+"', '" + weather_data['기온']+"', '"+weather_data['강수확률']+"', '"+
                        weather_data['하늘']+"') ON DUPLICATE KEY UPDATE tmp='" +weather_data['기온'] + "'," + 
                        "rain='" + weather_data['강수확률']+"'," + "sky='" + weather_data['하늘']+"';")
            db.commit()
            count = 0
    print(local + "data sended")

if __name__ == '__main__':
    host = "172.17.0.3"
    port = "27017"
    mongo = MongoClient(host, int(port))
    print(mongo)
    insert_item_one(mongo, {"name":"dogyu" }, "alarm", "test")
    insert_item_many(mongo, [{"name":"dogyu", "content":"nothing" }, {"name":"min", "content":"true"}], "alarm", "test")
