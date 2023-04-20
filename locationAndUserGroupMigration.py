import mysql.connector
import requests
import json

stateList = {"CA":"California","CT":"Connecticut","FL":"Florida","GA":"Georgia","NC":"North Carolina","NY":"New York","NJ":"New Jersey","OH":"Ohio","Pennsylvania":"Pennsylvania","PA":"Pennsylvania","Utah":"Utah","Kings":"Kings","New York":"New York","Ohio":"Ohio","null":None,"Null":None,"NULL":None,"Ny":"New York","ny":"New York","UT":"Utah","ut":"Utah","Ut":"Utah","NY \x08":"New York","NY \x07":"New York","NY \b":"New York","Nj":"New Jersey","State":"State","CO":"Colorado"}
serverUrl = "server url"
API_KEY = "xAuthToken"


mydb=mysql.connector.connect(
    host="ip",
    port="port",
    user="username",
    password="pw",
    database="dbname"
)

try:
    mycursor=mydb.cursor()
    mycursor.execute("select address1,address2,city,state,zip,country,description,name,primary_phone_no,date_created,last_updated,external_id,active_status from location where type='LOCATION'")
    myresult=mycursor.fetchall()
    for x in myresult:
        print "\n\n------------------------------------------------"
        address1 = x[0]
        address2 = x[1]
        city = x[2]
        state = x[3]
        zipcode = x[4]
        country = x[5]
        description = x[6]
        name = x[7]
        primary_phone_no = x[8]
        date_created = x[9]
        last_updated = x[10]
        external_id = x[11]
        active_status = x[12]
        print "Working wih location "+name+" and identifier "+external_id
        API_ENDPOINT = "https://"+serverUrl+"/minerva/Location/search"
        headers = {"x-auth-token":API_KEY}
        data = {"constraints":{"identifier":external_id}}
        jsonObject = json.dumps(data)
        response = requests.post(url=API_ENDPOINT, headers=headers, data=jsonObject)
        locationSearchResponseJson = response.json()
        if response.status_code == 200:
            if locationSearchResponseJson['totalCount']== 1:
                if(locationSearchResponseJson['list'] != [] and locationSearchResponseJson['list'] != None):
                    print "Location found in minerva with _id {}".format(locationSearchResponseJson['list'][0]['id'])
                    addressFlag = False
                    telecomFlag = False
                    locNameFlag = False
                    if (locationSearchResponseJson['list'][0]['address'] == [] or locationSearchResponseJson['list'][0]['address'] == None) and state != None and state.strip() != "" and stateList[state]!= None and zipcode != None and zipcode != "null" and zipcode != "Null":
                        addressFlag = True
                        locationSearchResponseJson['list'][0]['address'] = [{"line":[address1,address2],"state":stateList[state],"postalCode":zipcode,"city":city,"country":"USA","text":"{},{}, {}, {}, USA, {}".format(address1,address2,city,stateList[state],zipcode)}]
                    if (locationSearchResponseJson['list'][0]['telecom'] == [] or locationSearchResponseJson['list'][0]['telecom'] == None) and primary_phone_no != None and primary_phone_no !="null" and primary_phone_no !="Null" and primary_phone_no.strip() != "":
                        locationSearchResponseJson['list'][0]['telecom'] = [{"countryCode":"+1","system":"phone","useCode":"work","value":primary_phone_no}]
                        telecomFlag = True
                    if "name" not in locationSearchResponseJson['list'][0] :
                        locName = {'name':name}
                        locationSearchResponseJson['list'][0].update(locName)
                        locNameFlag = True
                    if telecomFlag or addressFlag or locNameFlag:
                        print "Updating name and/or address and/or telecom details"
                        locationUpdateUrl = "https://"+serverUrl+"/minerva/Location/update/"+str(locationSearchResponseJson['list'][0]['id'])
                        locationUpdateJson = json.dumps(locationSearchResponseJson['list'][0])
                        locationUpdateResponse = requests.put(url=locationUpdateUrl, headers=headers, data=locationUpdateJson)
                        if locationUpdateResponse.status_code == 200:
                            print "Location updated successfully"
                        else:
                            print "Unable to update Location. Api response {}".format(locationUpdateResponse.status_code)

                    print "Going to create userGroup"
                    userGroupCreateUrl = "https://"+serverUrl+"/minerva/moUserGroup/save"
                    userGroupCreateHeaders = {"x-auth-token":API_KEY,"Content-Type":"application/json","api-info":"V1|appVerson|deviceBrand|deviceModel|deviceScreenResolution|deviceOs|deviceOsVersion|deviceNetworkProvider|deviceNetworkType"}

                    userGroupCreateData = {"name":locationSearchResponseJson['list'][0]['name'],"agreements":[],"menuItems":[{"menuItem":5,"isDefault":True},{"menuItem":2,"isDefault":False}],"patientBannerMenuItems":[{"menuItem":41,"isDefault":True},{"menuItem":39,"isDefault":False}],"usersList":[],"authorities":{"ROLE_LOCATION_ACCESS":[locationSearchResponseJson['list'][0]['id']],"ROLE_REPORT_DOWNLOAD":"","ROLE_REPORT_PRINT":"","ROLE_PASSWORD_CHANGE":"","ROLE_ACCESS_PATIENT_BTG":"","ROLE_PATIENT_CONSENT":"","ROLE_EXTERNAL_REPORT_SHARE":"","ROLE_INTERNAL_REPORT_SHARE":"","ROLE_VIEW_PATIENT":"","ROLE_VIEW_PHYSICIAN":"","ROLE_VIEW_RESULTS":"","ROLE_CALL_PATIENT":"","ROLE_SEND_FAX":"","ROLE_PATIENT_ADVANCE_SEARCH":"","ROLE_CREATE_PATIENT":"","ROLE_BOOK_HOME_APPOINTMENTS":""}}
                    userGroupCreateJson = json.dumps(userGroupCreateData)
                    userGroupCreateResponse = requests.post(url=userGroupCreateUrl, headers=userGroupCreateHeaders, data=userGroupCreateJson)
                    if userGroupCreateResponse.status_code == 200:
                        print "UserGroup created successfully"
                    else:
                        print "Unable to create userGroup. Api response {}".format(userGroupCreateResponse.status_code)
            elif locationSearchResponseJson['totalCount']== 0:
                print "API returned no record"
                if name != None and external_id != None:
                    print "Going to Create Location"
                    locationCreateUrl = "https://"+serverUrl+"/minerva/Location/save"
                    locationCreateData = {"mode":"physical","name":{"value":name},"identifier":[{"value":external_id,"useCode":"usual","type":{"text":"performingLocationId"}}],"description":description,"status":"active","managingOrganization":{"id":1},"photo":[],"map":[],"physicalType":{"coding":[{"code":"si","display":"Site"}],"text":"Site"},"extension":[]}
                    if (state != None and state.strip() != "") and stateList[state]!= None and zipcode != None:
                        locationCreateData['address'] = [{"line":[address1,address2],"state":stateList[state],"postalCode":zipcode,"city":city,"country":"USA","text":"{},{}, {}, {}, USA, {}".format(address1,address2,city,stateList[state],zipcode)}]
                    if primary_phone_no != None and primary_phone_no.strip() != "":
                        locationCreateData['telecom'] = [{"countryCode":"+1","system":"phone","useCode":"work","value":primary_phone_no}]
                    locationCreateJson = json.dumps(locationCreateData)
                    locationCreateResponse = requests.post(url=locationCreateUrl, headers=headers, data=locationCreateJson)
                    if locationCreateResponse.status_code == 200:
                        print "Location created successfully"

                        print "Going to create userGroup"
                        locationCreateResponseJson = locationCreateResponse.json()
                        userGroupCreateUrl = "https://"+serverUrl+"/minerva/moUserGroup/save"
                        userGroupCreateHeaders = {"x-auth-token":API_KEY,"Content-Type":"application/json","api-info":"V1|appVerson|deviceBrand|deviceModel|deviceScreenResolution|deviceOs|deviceOsVersion|deviceNetworkProvider|deviceNetworkTyp"}
                        userGroupCreateData = {"name":locationCreateResponseJson['name'],"agreements":[],"menuItems":[{"menuItem":5,"isDefault":True},{"menuItem":2,"isDefault":False}],"patientBannerMenuItems":[{"menuItem":41,"isDefault":True},{"menuItem":39,"isDefault":False}],"usersList":[],"authorities":{"ROLE_LOCATION_ACCESS":[locationCreateResponseJson['id']],"ROLE_REPORT_DOWNLOAD":"","ROLE_REPORT_PRINT":"","ROLE_PASSWORD_CHANGE":"","ROLE_ACCESS_PATIENT_BTG":"","ROLE_PATIENT_CONSENT":"","ROLE_EXTERNAL_REPORT_SHARE":"","ROLE_INTERNAL_REPORT_SHARE":"","ROLE_VIEW_PATIENT":"","ROLE_VIEW_PHYSICIAN":"","ROLE_VIEW_RESULTS":"","ROLE_CALL_PATIENT":"","ROLE_SEND_FAX":"","ROLE_PATIENT_ADVANCE_SEARCH":"","ROLE_CREATE_PATIENT":"","ROLE_BOOK_HOME_APPOINTMENTS":""}}
                        userGroupCreateJson = json.dumps(userGroupCreateData)
                        userGroupCreateResponse = requests.post(url=userGroupCreateUrl, headers=userGroupCreateHeaders, data=userGroupCreateJson)
                        if userGroupCreateResponse.status_code == 200:
                            print "UserGroup created successfully"
                        else:
                            print "Unable to create userGroup. Api response {}".format(userGroupCreateResponse.status_code)
                    else:
                        print "Unable to create Location. Api response {}".format(locationCreateResponse.status_code)
                else:
                    print "Not creating location record for {} and identifier {}".format(name,external_id)
            else:
                print "Api returned multiple record",locationSearchResponseJson['list']
        else:
            print "Invalid Api Response ",response.status_code
    print("----------Script executed successfully------")
except Exception as e:
    print('Some error occured')
    print(e)
    quit()