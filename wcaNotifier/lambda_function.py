from boto3 import client, resource
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
from collections import defaultdict
from geopy.geocoders import GoogleV3
from geopy.distance import distance
from html import unescape
from ics import Calendar
from os import environ
from json import loads
from requests import get
from re import DOTALL, findall, search
from time import sleep
from timezonefinder import TimezoneFinder
from traceback import format_exc


def lambda_handler(event, context):
    try:
        with open("currencies.txt", "r", encoding="utf-8") as f:
            currencies = f.read().split('\n')
            currCodes = {}
            currDecs = {}
            for curr in currencies:
                name, code, decs = curr.split(',')
                currCodes[name] = code
                currDecs[code] = decs
            f.close()
    
        baseCurr = "USD"
        rates = loads(get(f"https://v6.exchangerate-api.com/v6/{environ['CURR_CONV_KEY']}/latest/{baseCurr}").text)["conversion_rates"]
    
        wcaComps = resource("dynamodb").Table("wcaCompetitions")
        loggedComps = wcaComps.get_item(Key={"comps": "competitions"})["Item"]["currComps"]
        page = 0
        newComps, currComps = set(), set()
        while True: # Loop just in case there are more than 25 new competitions announced between runs
            page += 1
            res = get(f"https://www.worldcubeassociation.org/api/v0/competition_index?include_cancelled=false&sort=-announced_at,name&page={page}")
            res.raise_for_status()
            currentComps = loads(res.text)
            for curComp in currentComps:
                compLink = f"/competitions/{curComp['id']}"
                if compLink not in loggedComps:
                    newComps.add(compLink)
                currComps.add(compLink)
            if len(newComps) < len(currComps):
                break
            sleep(0.5)
        print(currComps)
        wcaComps.put_item(Item={"comps": "competitions", "currComps": currComps})
        print(newComps)
        
        allUsers = resource("dynamodb").Table("notifierUsers")
        crossCountries = allUsers.query(IndexName="crossCountry-userUUID-index", KeyConditionExpression=Key("crossCountry").eq("yes"))["Items"]
        globalUsers = allUsers.query(IndexName="country-userUUID-index", KeyConditionExpression=Key("country").eq("null"))["Items"]
        
        geoLoc = GoogleV3(api_key=environ["GEOCODING_KEY"])
        userNotif = defaultdict(set)
        userCurr, userMail, compData = {}, {}, {}
        for checkComp in newComps:
            approxLoc = False
            unknownLoc = False
            comp = BeautifulSoup(get(f"https://www.worldcubeassociation.org{checkComp}").text, features="html.parser")
            name = unescape(findall(r"(?<=>).+?(?=<)", str(comp.find("h3")), flags=DOTALL)[-1].strip())
            rawDetails = comp.find("dl", class_="dl-horizontal compact").findAll("dd")
            compEvents = list(Calendar(get(f"https://www.worldcubeassociation.org{checkComp}.ics").text).timeline)
            evStart, evEnd = compEvents[0].begin, compEvents[-1].end
            city = search(r"(?<=>).+(?=<)", str(rawDetails[1])).group()
            country = city.split(", ")[-1]
            if country == "Chinese Taipei":
                country = "Taiwan"
            elif country == "China":
                area = city.split(", ")[-2]
                if area == "Hong Kong":
                    country = "Hong Kong"
                elif area == "Macau":
                    country = "Macau"
            lat, lng = map(float, search(r"-*\d+\.\d+,-*\d+\.\d+", str(rawDetails[3])).group().split(','))
            reqText = str(comp.find("div", id="registration_requirements_text"))
            if "Registering for this competition is free." not in reqText:
                try:
                    feeStr = search(r"(?<=fee for this competition is ).+\d+.+(?=.)", reqText).group()
                    origCurr = currCodes[search(r"(?<=\().+(?=\))", feeStr).group()]
                    origFee = float(search(r"\d+\.?\d*", feeStr).group())
                except AttributeError:
                    origCurr, origFee = None, None
            else:
                origCurr, origFee = None, 0.0
            events = set(findall(r'(?<=event-).+?(?=\")', str(comp.find("dd", class_="competition-events-list"))))
            if not (lat or lng):
                approxLoc = True
                loc = geoLoc.geocode(city.split(", ")[-1])
                if loc:
                    lat = loc.latitude
                    lng = loc.longitude
                else:
                    unknownLoc = True
            compData[checkComp] = {"name": name, "origFee": origFee, "origCurr": origCurr, "events": events, "start": evStart, "end": evEnd, "city": city, "country": country, "lat": lat, "lng": lng, "approxLoc": approxLoc, "unknownLoc": unknownLoc}
            
            checkUsers = allUsers.query(IndexName="country-userUUID-index", KeyConditionExpression=Key("country").eq(country))["Items"] + crossCountries + globalUsers
            for user in checkUsers:
                if user["isVerified"]:
                    if user["events"] & events and (not user["start"] or user["start"] <= evStart.timestamp()) and (not user["end"] or user["end"] >= evEnd.timestamp()):
                        if user["fee"] and origFee != None and origFee > 0.0:
                            decs = int(currDecs[origCurr])
                            destFee = float(user["fee"]) / rates[user["curr"]] * rates[origCurr]
                            destFee = int(destFee) if decs == 0 else round(destFee, decs)
                            userCurr[user["userUUID"]] = user["curr"]
                        if (user["country"] == "null" or (distance((user["latitude"], user["longitude"]), (lat, lng)).km <= user["radius"] + (3000 if approxLoc else 0)) or unknownLoc) and (not user["fee"] or origFee == None or destFee >= origFee):
                            userNotif[user["userUUID"]].add(checkComp)
                            userMail[user["userUUID"]] = user["email"]
            sleep(1)
        print(compData)
        
        eventTrans = {"222": "2x2x2 Cube", "333": "3x3x3 Cube", "444": "4x4x4 Cube", "555": "5x5x5 Cube", "666": "6x6x6 Cube", "777": "7x7x7 Cube", "333bf": "3x3x3 Blindfold", "444bf": "4x4x4 Blindfold", "555bf": "5x5x5 Blindfold", "333mbf": "3x3x3 Multi-Blind", "333fm": "3x3x3 Fewest Moves", "333oh": "3x3x3 One-handed", "clock": "Clock", "minx": "Megaminx", "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1"}
        ses = client("sesv2")
        tf = TimezoneFinder()
        emailUUIDs = {}
        emailNotif = defaultdict(set)
        for user, comps in userNotif.items():
            emailNotif[userMail[user]] |= comps
            emailUUIDs[userMail[user]] = user
        print(userNotif, emailNotif)
        for recip, comps in emailNotif.items():
            email = "<html><head></head><body><h1>WCA Competition Notification</h1><p>There are competitions that might be of relevance to you: (For more information on any of these competitions, click the hyperlink for the corresponding competition.)</p><ul>"
            for comp in comps:
                cData = compData[comp]
                compTz = tf.timezone_at(lat=cData["lat"], lng=cData["lng"])
                email += f"<li><a href=\"https://worldcubeassociation.org{comp}\">{cData['name']}</a><ul><li>Location: {cData['city']}</li><li>Events: {', '.join([eventTrans[i] for i in cData['events']])}</li><li>Competition Period: {cData['start'].to(compTz).format('YYYY-MM-DD HH:mm:ss [GMT]ZZ')} - {cData['end'].to(compTz).format('YYYY-MM-DD HH:mm:ss [GMT]ZZ')}</li>"
                origFee = cData["origFee"]
                if origFee != None:
                    if origFee > 0.0:
                        origDecs = int(currDecs[cData["origCurr"]])
                        origFee = int(origFee) if origDecs == 0 or origFee.is_integer() else round(origFee, origDecs)
                        email += f"<li>Fee: {origFee} {cData['origCurr']}"
                        if user in userCurr and userCurr[user] != cData["origCurr"]:
                            destDecs = int(currDecs[userCurr[user]])
                            destFee = float(cData["origFee"]) / rates[cData["origCurr"]] * rates[userCurr[user]]
                            destFee = int(destFee) if destDecs == 0 or destFee.is_integer() else round(destFee, destDecs)
                            email += f" ({destFee} {userCurr[user]})</li></ul>{'<strong>Note that this event might be sent in error due to the organizer not giving a proper address or it has multiple addresses.</strong><br><br>' if (cData['approxLoc'] or cData['unknownLoc']) else '<br>'}</li>"
                        else:
                            email += f"</li></ul>{'<strong>Note that this event might be sent in error due to the organizer not giving a proper address or it has multiple addresses.</strong><br><br>' if (cData['approxLoc'] or cData['unknownLoc']) else '<br>'}</li>"
                    else:
                        email += f"<li>Fee: Free</li></ul>{'<strong>Note that this event might be sent in error due to the organizer not giving a proper address or it has multiple addresses.</strong><br><br>' if (cData['approxLoc'] or cData['unknownLoc']) else '<br>'}</li>"
                else:
                    email += f"<li>No Fee Data Available.</li></ul>{'<strong>Note that this event might be sent in error due to the organizer not giving a proper address or it has multiple addresses.</strong><br><br>' if (cData['approxLoc'] or cData['unknownLoc']) else '<br>'}</li>"
            email += f"</ul><br><p>To Opt-out of these notification emails, <a href=\"https://m4q4s5pxsarghztkazrode4mdq0dtfli.lambda-url.us-east-2.on.aws/?uuid={emailUUIDs[recip]}\">click here</a>.<br><br><br>To report any issues, please go <a href=\"https://github.com/miclol/WCANotifier/issues\">here</a>.</p></body></html>"
            ses.send_email(Destination={"ToAddresses": [recip]}, Message={"Simple": {"Body": {"Html": {"Data": email}}, "Subject": {"Data": "WCA Notification"}, "Headers": [{"Name": "List-Unsubscribe", "Value": f"<https://m4q4s5pxsarghztkazrode4mdq0dtfli.lambda-url.us-east-2.on.aws/?uuid={emailUUIDs[recip]}>"}, {"Name": "List-Unsubscribe-Post", "Value": "List-Unsubscribe=One-Click"}]}}, FromEmailAddress="wcaalert@gmail.com")
            sleep(0.1)
    except:
        ses.send_email(Destination={"ToAddresses": ["wcaalert@gmail.com"]}, Message={"Simple": {"Body": {"Text": {"Data": format_exc()}}, "Subject": {"Data": "WCA Notification Failed!"}}}, FromEmailAddress="wcaalert@gmail.com")