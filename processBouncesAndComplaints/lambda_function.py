from boto3 import resource
from boto3.dynamodb.conditions import Key
from json import loads


def lambda_handler(event, context):
    msg = loads(event["Records"][0]["Sns"]["Message"])
    print(msg)
    problemType = msg["notificationType"]
    problemEmail = msg["mail"]["destination"][0]
    if problemType == "Bounce":
        if msg["bounce"]["bounceType"] != "Permanent":
            return
        else:
            bouncedEmails = resource("dynamodb").Table("bouncedEmails")
            bouncedEmails.update_item(Key={"emails": "emails"}, UpdateExpression="ADD bouncedEmails :email", ExpressionAttributeValues={":email": set([problemEmail.lower()])})
    users = resource("dynamodb").Table("notifierUsers")
    allSignups = users.query(IndexName="email-userUUID-index", KeyConditionExpression=Key("email").eq(problemEmail))["Items"]
    for signup in allSignups:
        users.delete_item(Key={"userUUID": signup["userUUID"]})
