from boto3 import resource
from boto3.dynamodb.conditions import Key
from re import fullmatch
from urllib.parse import parse_qs


def lambda_handler(event, context):
    httpSource = event["requestContext"]["http"]
    if httpSource["path"] == "/":
        if httpSource["method"] == "GET":
            try:
                payload = parse_qs(event["rawQueryString"])
                uuid = payload["uuid"][0]
                if not fullmatch(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uuid):
                    return {"statusCode": 403, "body": "Invalid UUID!"}
                if len(payload.keys()) > 1:
                    raise Exception
                users = resource("dynamodb").Table("notifierUsers")
                user = users.query(KeyConditionExpression=Key("userUUID").eq(uuid))["Items"]
                print(user)
                if len(user) == 1:
                    if not user[0]["isVerified"]:
                        end = user[0]["end"]
                        users.update_item(Key={"userUUID": uuid}, UpdateExpression="SET #ver = :ver, #ttl = :ttl", ExpressionAttributeNames={"#ver": "isVerified", "#ttl": "ttl"}, ExpressionAttributeValues={":ver": True, ":ttl": end if end else None})
                        print("we good")
                        return {"statusCode": 200, "headers": {"content-type": "text/html"}, "body": "Thank you for verifying your email. You will now recieve notifications."}
                    else:
                        return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "You have already been verified!"}
                else:
                    return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid UUID!"}
            except:
                return {"statusCode": 403, "headers": {"content-type": "text/html"}, "body": "Invalid Format!"}
        else:
            return {"statusCode": 405, "headers": {"content-type": "text/html"}, "body": "Invalid HTTP Method!"}
    else:
        return {"statusCode": 404}
