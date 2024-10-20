from boto3 import client, resource
from boto3.dynamodb.conditions import Key
from base64 import b64decode
from decimal import Decimal
from dns import resolver
from os import environ
from json import loads, dumps
from re import fullmatch
from time import time
from urllib.parse import parse_qs
from urllib3 import PoolManager
from uuid import uuid4


def lambda_handler(event, context):
    httpSource = event["requestContext"]["http"]
    print(httpSource)
    if httpSource["path"] == "/":
        headers = event["headers"]
        if "origin" not in headers:
            return {"statusCode": 401}
        else:
            if headers["origin"] != "https://miclol.github.io":
                return {"statusCode": 401}
        if httpSource["method"] == "POST":
            payload = parse_qs(b64decode(event["body"]).decode())
            for k, v in payload.items():
                if len(v) == 1:
                    payload[k] = v[0]
            try:
                pKeys = payload.keys()
                allKeys = {"email", "events", "latitude", "longitude", "country", "radius", "crossCountry", "fee", "curr", "start", "end", "g-recaptcha-response"}
                if any([x not in allKeys for x in pKeys]):
                    raise Exception
                for aK in allKeys:
                    if aK not in pKeys:
                        payload[aK] = None
                for k, v in payload.items():
                    if k != "g-recaptcha-response":
                        globals()[k] = v
                resp = PoolManager().request(method="POST", url=f"https://www.google.com/recaptcha/api/siteverify?secret={environ['CAPTCHA_KEY']}&response={payload['g-recaptcha-response']}")
                if not loads(resp.data)["success"]:
                    return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Captcha Key!"}
                payload.pop("g-recaptcha-response")
                payload["events"] = set(events.split(','))
                if payload["events"].isdisjoint({"222", "333", "444", "555", "666", "777", "333bf", "444bf", "555bf", "333mbf", "333fm", "333oh", "clock", "minx", "pyram", "skewb", "sq1"}):
                    raise Exception
                start, end = payload["start"], payload["end"]
                payload["start"] = Decimal(int(start)) if start else None
                payload["end"] = Decimal(int(end)) if end else None
                if country:
                    with open("countries.txt", "r") as f:
                        countries = {}
                        co = f.read().split('\n')
                        for c in co:
                            x = c.split(',')
                            countries[x[1]] = x[0]
                        f.close()
                    if country not in countries:
                        return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Country!"}
                    else:
                        payload["country"] = countries[country]
                    payload["latitude"] = Decimal(latitude)
                    payload["longitude"] = Decimal(longitude)
                    payload["radius"] = Decimal(int(radius))
                    if crossCountry not in {"yes", "no"}:
                        raise Exception
                    if fee:
                        if not curr:
                            raise Exception
                        with open("currencies.txt", "r") as f:
                            currDecs = {}
                            currs = f.read().split('\n')
                            for c in currs:
                                x = c.split(',')
                                currDecs[x[1]] = int(x[2])
                            f.close()
                        if not currDecs[curr]:
                            payload["fee"] = int(fee)
                        else:
                            try:
                                payload["fee"] = int(fee)
                            except:
                                payload["fee"] = Decimal(fee)
                        if type(payload["fee"]) == Decimal and len(str(fee).split('.')[1]) > currDecs[curr]:
                            return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Value for Fee!"}
                    else:
                        payload["curr"] = None
                else:
                    if crossCountry or latitude or longitude or radius or fee:
                        raise Exception
                    payload["crossCountry"] = "null"
                    payload["country"] = "null"
                bouncedEmails = resource("dynamodb").Table("bouncedEmails")
                if not fullmatch(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}$", email) or email.lower() in bouncedEmails.get_item(Key={"emails": "emails"})["Item"]["bouncedEmails"]:
                    return {"statusCode": 403, "body": "Invalid Email! Perhaps you typed it in wrong?"}
                try:
                    resolver.resolve(email.split('@')[1], "MX")
                except resolver.NXDOMAIN:
                    return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Email!"}
                if start and end and start > end:
                    return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Start/End Time Part! Perhaps you typed it in wrong?"}
            except Exception as e:
                return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Format!"}
            uuid = str(uuid4())
            payload["userUUID"] = uuid
            payload["isVerified"] = False
            payload["ttl"] = Decimal(int(time()) + 86400)
            users = resource("dynamodb").Table("notifierUsers")
            alreadyReg = users.query(IndexName="email-userUUID-index", KeyConditionExpression=Key("email").eq(email))["Items"]
            equalCheckSubset = {"events", "latitude", "longitude", "radius", "crossCountry", "fee"} 
            for ar in alreadyReg:
                if not ar["isVerified"]:
                    return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Please verify your email of a request you made before before making more requests."}
                if {x: payload[x] for x in equalCheckSubset} == {x: ar[x] for x in equalCheckSubset}:
                    start, end = payload["start"], payload["end"]
                    if ((start and ar["start"] and start >= ar["start"]) or not (start or ar["start"])) and ((end and ar["end"] and end <= ar["end"]) or not (end or ar["end"])):
                        return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "A similar request has already been made so no action will be taken."}
            print(payload)
            users.put_item(Item=payload)
            content = f"<html><head></head><body><h1>WCA Notifier Verification Email</h1><p>To verify that you are actually trying to recieve notifications for WCA Competitions, please confirm <a href=\"https://nfty5lb2qe4bbhpwmul7u6gp6m0ffohy.lambda-url.us-east-2.on.aws/?uuid={uuid}\">here</a>. This verification link will expire in 24 hours.<br><br>If you did not sign up for notifications, do not click the link, as somebody probably mistyped their email.<br>If the hyperlink doesn't work, go to this link: https://nfty5lb2qe4bbhpwmul7u6gp6m0ffohy.lambda-url.us-east-2.on.aws/?uuid={uuid}<br><hr><br>To Opt-out of these notification emails, <a href=\"https://m4q4s5pxsarghztkazrode4mdq0dtfli.lambda-url.us-east-2.on.aws/?uuid={uuid}\">click here</a>.<br><br>To report any issues, please go <a href=\"https://github.com/miclol/WCANotifier/issues\">here</a>.</p></body></html>"
            client("ses").send_email(Destination={"ToAddresses": [email]}, Message={"Body": {"Html": {"Data": content}}, "Subject": {"Data": "WCA Notifier Verification Email"}}, Source="wcaalert@gmail.com")
            return {"statusCode": 200, "headers": {"content-type": "text/html"}, "body": "Thank you for using this service. Please check your email for a verification email."}
        else:
            return {"statusCode": 405, "headers": {"content-type": "text/html"}, "body": "Invalid HTTP Method!"}
    else:
        return {"statusCode": 404}
