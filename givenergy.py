import requests
import json
import datetime

key="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5NTc3MDIxOS1jYWE2LTRmOTctOTE3Ni0zNDBlZGMzZDQxNTgiLCJqdGkiOiIwZDJmOTc0NGQyMjg5NjU0ZTliMTQzZmY4YTEzOGM5MGQxOTExMDc4OTA4OTcwOWRjNDNmNTFiMzkxODdmMmFjYjA0MmFhMzllMWEyNTA5ZCIsImlhdCI6MTY5NzExNzg2NS42NDEyNTcsIm5iZiI6MTY5NzExNzg2NS42NDEyNjEsImV4cCI6MzI1MDM2ODAwMDAuMDA4MjEzLCJzdWIiOiIyMjgyNCIsInNjb3BlcyI6WyJhcGk6aW52ZXJ0ZXIiXX0.txcfcLQ9StXWcXiGdq-GXm1_DNC4XB7-AFB0r7F8Tob1DcItEHCc4D_D2gsK9BW3nQM3SYHk82KLQ-u6hd5lnQ"
inverter="SD2237G385"
reserve=25
discharge_time=23
discharge_stop=2
charge_start=2
charge_stop=6

headers = {
          'Authorization': key,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
          }


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

def get_settings():
	url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings'
	response = requests.request('GET', url, headers=headers)
	print(response.json())

def get_battery():
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/system-data/latest'
        response = requests.request('GET', url, headers=headers)
        out=response.json()['data']['battery']['percent']
        return out


def set_discharge():
        #eco mode 24: F

        value=False
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/24/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

        #Enable DC discharge 56: T
        value=True
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/56/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

	#DC Discharge 1 End Time 54: 23:59
        value="01:59"
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/54/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())



def set_eco():
        #eco mode 24

        value=True
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/24/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

        #Enable DC discharge 56: T
        value=False
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/56/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

        #DC Discharge 1 End Time 54: 23:59
        #value="00:00"
        #url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/54/write'
        #payload = {
        #          "value": value
        #          }
        #response = requests.request('POST', url, headers=headers, json=payload)
        #jprint(response.json())



def set_charging():
        # 66 Enable AC charge
        value=True
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/66/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

 
        # 64 AC charge 1 start time
        value="02:00"
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/64/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())

        # 65 AC charge 1 end time
        value="06:00"
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/65/write'
        payload = {
                  "value": value
                  }
        response = requests.request('POST', url, headers=headers, json=payload)
        jprint(response.json())



def check_state():
        #eco
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/24/read'
        response = requests.request('post', url, headers=headers)
        eco=response.json()['data']['value']

        #discharge
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/56/read'
        response = requests.request('post', url, headers=headers)
        discharge=response.json()['data']['value']

        #time
        url = 'https://api.givenergy.cloud/v1/inverter/' + inverter + '/settings/54/read'
        response = requests.request('post', url, headers=headers)
        time=response.json()['data']['value']
        mode=""
        if ((eco == True) and (discharge == False)):
            mode="eco"
            print(" Actual mode: eco ", end=" ")
        if ((eco == False) and (discharge == True)):
            mode="discharge"
            print(" Actual mode: discharge ", end=" ")
        return mode








#set_discharge()



#battery = get_battery()
#print(battery)




#y=set_charging()
x = datetime.datetime.now()
hour=x.hour
print(hour, end=" ")
battery=get_battery()
mode=check_state()
if (   (battery > reserve) and (   (hour >= discharge_time) or (hour <= discharge_stop)    )    ):
        print(" required mode: discharging ", end=" ")
        if (mode != "discharging"):
             set_discharge()
else:
        print(" required mode: eco ", end=" ")
        if (mode != "eco"):
             set_eco();

print("\n")

