import mysql.connector
import requests
import json
import random
import string
import csv
from datetime import datetime

now = datetime.now()
print "start_time ...",now

serverUrl = "server url"
API_KEY = "xauthToken"
headers = {"x-auth-token":API_KEY,"api-info":"api-info"}

physicianMasterRecord = []
userMasterRecord = []

mydb=mysql.connector.connect(
    host="ip",
    port="port",
    user="user",
    password="pw",
    database="dbname"
)

def getUserGroupMethod(external_id):
    userGroupId = None
    userGroupName = None
    if external_id == "Internal Physician" or external_id == "Ordering Physician" :
        userGroupName = external_id
    else :
        locationSearchUrl = "https://"+serverUrl+"/minerva/Location/search"
        locationSearchData = {"constraints":{"identifier":external_id}}
        jsonObject = json.dumps(locationSearchData)
        response = requests.post(url=locationSearchUrl, headers=headers, data=jsonObject)
        locationSearchResponseJson = response.json()  
        if response.status_code == 200:
            if locationSearchResponseJson['totalCount'] >= 1:
                userGroupName = locationSearchResponseJson['list'][0]['name']
    
    userGroupSearchUrl = "https://"+serverUrl+"/minerva/moUserGroup/search"
    userGroupSearchData = {"searchString":userGroupName,"random":"582.6246240872428"}
    userGroupSearchJson = json.dumps(userGroupSearchData)
    userGroupSearchResponse = requests.post(url=userGroupSearchUrl, headers=headers, data=userGroupSearchJson)
    if userGroupSearchResponse.status_code == 200:
        userGroupSearchResponseJson = userGroupSearchResponse.json()
        if userGroupSearchResponseJson['totalCount'] >= 1:
            userGroupId = userGroupSearchResponseJson['resultList'][0]['id']
    return userGroupId
    
def get_random_string():
    letters = string.ascii_letters + string.digits
    result_str = ''.join(random.choice(letters) for i in range(8))
    return result_str

try:
    mycursor=mydb.cursor()
    mycursor.execute("select npi,address1,address2,city,external_id,first_name,last_name,middle_name,physician_type,state,user_id,zip,location_id,full_name,id,email_address from mpi_physician where is_deleted = 0")
    myresult=mycursor.fetchall()
    for x in myresult:
        print "\n\n------------------------------------------------" 
        npi = x[0]
        address1 = x[1]
        address2 = x[2]   
        city     = x[3]                              
        external_id = x[4]
        first_name = x[5]
        lowerCase_first_name = ""
        lowerCase_last_name = ""
        lowerCase_middle_name = ""
        lowerCase_full_name = ""
        if first_name!= None:
            lowerCase_first_name = first_name.lower()
        last_name  = x[6]
        if last_name!= None:
            lowerCase_last_name = last_name.lower()
        middle_name = x[7]
        if middle_name!= None:
            lowerCase_middle_name = middle_name.lower()
        physician_type = x[8]                  
        state = x[9]
        user_id = x[10]      
        zipcode = x[11]                
        performing_location_id = x[12]  
        full_name = x[13]
        if full_name != None:
            lowerCase_full_name = full_name.lower()
        mpi_physician_id = x[14]
        email_address = x[15]
        performingLocationId = None
        print "Working with physician {} having external_id {}".format(first_name + " "+ last_name,external_id)
        mycursor.execute("select external_id,full_location_access,location_id from physician where mpi_physician_id = "+ str(mpi_physician_id))
        myresult2=mycursor.fetchall()
        print "Total entries found for {} in physician table is {}".format(first_name + " "+ last_name,len(myresult2))
        identifierDeatils = []
        
        if performing_location_id != None:
            mycursor.execute("select external_id from location where id = {}".format(performing_location_id))
            myresult3=mycursor.fetchall()
            if len(myresult3) == 0:
                continue
            elif myresult3[0][0] != None:
                locationSearchUrl = "https://"+serverUrl+"/minerva/Location/search"
                locationSearchData = {"constraints":{"identifier":myresult3[0][0]}}
                jsonObject = json.dumps(locationSearchData)
                response = requests.post(url=locationSearchUrl, headers=headers, data=jsonObject)
                locationSearchResponseJson = response.json()  
                if response.status_code == 200:
                    if locationSearchResponseJson['totalCount'] >= 1:
                        performingLocationId = locationSearchResponseJson['list'][0]['id']

        if npi != None:
            identifierDeatils = [{"useCode":"official","extension":[],"system":"NPI","type":{"coding":[{"extension":[],"code":"npi"}],"extension":[],"text":"National provider identifier"},"value":npi,"regexValidation":"^(?=.{1,20}$).*"}]
            
        if len(myresult2) > 0:
            physicianCounter = 0
            updatePhysician = None
            ugLocationIdList = []
            orderingPhysicianFlag = False
            physicianRecord = []
            for physician in myresult2:
                linked_external_id = physician[0]
                full_location_access = physician[1]
                location_id = physician[2]
                getUserGroup = False
                if physician_type == 'External':
                    getUserGroup = True
                
                print "Searhing for physician with identifier {}".format(linked_external_id)
                physicianSearchUrl = "https://"+serverUrl+"/minerva/moPractitioner/fetchPractitioners"
                data = {"constraints":{"identifier:text":linked_external_id}}
                jsonObject = json.dumps(data)
                response = requests.post(url=physicianSearchUrl, headers=headers, data=jsonObject) 
                if response.status_code == 200:                    
                    physicianSearchResponseJson = response.json() 
                    if(physicianSearchResponseJson['practitioners'] != [] and physicianSearchResponseJson['practitioners'] != None):
                        print "Physician found in minerva with _id {}".format(physicianSearchResponseJson['practitioners'][0]['id'])
                        if physicianCounter == 0:
                            updatePhysician = physicianSearchResponseJson['practitioners'][0]
                        physicianCounter = physicianCounter+1
                        physicianRecord.append(physicianSearchResponseJson['practitioners'][0]['id'])
                    if getUserGroup == True:
                        if full_location_access == "on":
                            ugLocationIdList.append(location_id)
                        else:
                            orderingPhysicianFlag = True
                else :
                     print "Physician not found in minerva with identifier {}".format(linked_external_id)
                identifierDeatils.append({"type":{"coding":[],"extension":[],"text":"physicianId"},"useCode":"usual","value":linked_external_id,"enableEditIdentifier":True,"regexValidation":"^(?=.{1,20}$).*","showEditButton":True})     
              
            if physicianCounter>0:
                print "Physician data will be updated in fetched physician on minerva. No new physician will be created"
            
            if physicianRecord != []:
                physicianMasterRecord.append(physicianRecord)
            
            if updatePhysician != None:
                updatePhysicianUrl = "https://"+serverUrl+"/minerva/Practitioner/update/"+str(updatePhysician['id'])
                updatePhysician['identifier'] = identifierDeatils
                if lowerCase_middle_name != None:
                    updatePhysician['lowerCaseName']={"given":[lowerCase_first_name +" "+lowerCase_middle_name],"useCode":"Human Name","language":"en","text":lowerCase_full_name,"family":[lowerCase_last_name]}
                else:
                    updatePhysician['lowerCaseName']={"given":[lowerCase_first_name],"useCode":"Human Name","language":"en","text":lowerCase_full_name,"family":[lowerCase_last_name]}
                if performingLocationId != None:
                    updatePhysician['location'] = [{"id":performingLocationId,"ref":"https://"+serverUrl+"/minerva/Location/show/"+str(performingLocationId),"class":"com.mphrx.consus.resources.Location"}]
                
                del updatePhysician['lastUpdated']
                del updatePhysician['createdDate']
                physicianUpdateJson = json.dumps(updatePhysician)
                physicianUpdateResponse = requests.put(url=updatePhysicianUrl, headers=headers, data=physicianUpdateJson)
                
                if physicianUpdateResponse.status_code == 200:
                    print "physician updated successfully"
                else :
                    print "physician not updated successfully",physicianUpdateResponse.status_code,updatePhysician
                
            else :
                print "Physician not found. Going to create physician"
                createPhysicianUrl = "https://"+serverUrl+"/minerva/Practitioner/save"
                if middle_name!=None:
                    nameDetail = [{"family":last_name,"given":first_name+" "+middle_name,"prefix":[],"suffix":[],"text":first_name+" "+middle_name+" "+last_name,"useCode":"Human Name"}]
                else :
                    nameDetail = [{"family":last_name,"given":first_name,"prefix":[],"suffix":[],"text":first_name+" "+last_name,"useCode":"Human Name"}]
                createPhysicianData = {"communication":[],"extension":[],"active":True,"gender":"","identifier":identifierDeatils,"name":nameDetail,"practitionerRole":[{"specialty":[],"role":{"text":""},"managingOrganization":{"id":1,"ref":"https://"+serverUrl+"/minerva/Organization/show/1","class":"com.mphrx.consus.resources.Organization"},"healthcareService":[]}],"photo":[],"qualification":[],"location":[{"id":performingLocationId,"ref":"https://"+serverUrl+"/minerva/Location/show/"+str(performingLocationId),"class":"com.mphrx.consus.resources.Location"}]}
                
                physicianCreateJson = json.dumps(createPhysicianData)
                physicianCreateResponse = requests.post(url=createPhysicianUrl, headers=headers, data=physicianCreateJson)
                
                if physicianCreateResponse.status_code == 200:
                    print "physician created successfully"
                    updatePhysician = physicianCreateResponse.json()
                else:
                    print "Unable to create physician"
                            
            if user_id != None and updatePhysician != None:
                userRecord = []
                print "Going to migrate user"
                mycursor.execute("select email,user_real_name,username,phone_number from person where id = {} and user_type = 'Physician' and enabled = true".format(user_id))
                myresult4=mycursor.fetchall()
                if(len(myresult4)>0):
                    email = myresult4[0][0]
                    user_real_name = myresult4[0][1]
                    username = myresult4[0][2]
                    nameList = user_real_name.split()
                    user_first_name = ""
                    user_last_name = ""
                    if len(nameList)>1:
                        user_first_name = ' '.join(tuple(nameList[:len(nameList)-1]))
                        user_last_name = ' '.join(tuple(nameList[len(nameList)-1:]))
                    else:
                        user_first_name = user_real_name
                        
                    password = get_random_string()
                    phone_number = myresult4[0][3]
                    
                    userGroupIdList = []
                    if getUserGroup == False:
                        userGroupIdList.append(getUserGroupMethod("Internal Physician"))
                    else:
                        if orderingPhysicianFlag == True:
                            userGroupIdList.append(getUserGroupMethod("Ordering Physician"))
                        
                        if(len(ugLocationIdList)>0):
                            if len(ugLocationIdList) == 1:
                                mycursor.execute("select external_id from location where id = {}".format(ugLocationIdList[0]))
                            else:
                                mycursor.execute("select external_id from location where id in {}".format(tuple(ugLocationIdList)))
                            myresult5=mycursor.fetchall()
                            for each_external_id in myresult5:
                                userGroupIdList.append(getUserGroupMethod(each_external_id))
                    
                    userCreateUrl = "https://"+serverUrl+"/minerva/moUser/save"
                    userCreateData = {"userGroups":userGroupIdList,"username":username,"email":email,"password":password,"firstName":user_first_name,"lastName":user_last_name,"phoneNo":phone_number,"countryCode":"+1","gender":"","dob":"","activeResource":{"status":"ACTIVE"},"enabled":True,"userType":"PRACTITIONER","adminCreatedPasswordNeverChanged":True,"signUpSource":"MINERVA","physicianId":updatePhysician['id']}
                    userCreateJson = json.dumps(userCreateData)
                    userCreateResponse = requests.post(url=userCreateUrl, headers=headers, data=userCreateJson)
                    
                    if userCreateResponse.status_code == 200:
                        print "User created successfully"
                        userRecord.append(username)
                        userRecord.append(password)
                        userRecord.append(email)
                        userRecord.append(phone_number)
                        userMasterRecord.append(userRecord)
                    else:
                        print "Unable to Create User. Api response: {}".format(userCreateResponse.status_code)
                else:
                    print "No active physician user found"
            else:
                print "No user present for physician"
        else:
            print "No entry found in physician table"
    
    f = open('/tmp/physicianIdsForMerging.csv', 'w')
    writer = csv.writer(f)
    header = ["Physician merge ids"]
    writer.writerow(header)
    writer.writerows(physicianMasterRecord)

    f = open('/tmp/userPassword.csv', 'w')
    writer = csv.writer(f)
    header = ["username","password","email","phoneNo"]
    writer.writerow(header)
    writer.writerows(userMasterRecord)
    end = datetime.now()
    print "end_time ...",end
except Exception as e:
    print('Some error occured')
    print(e)
    quit()