# WCANotifier Serverside

The AWS side for [WCANotifier](https://github.com/miclol/WCANotifier/).

Some information about the server side:
- Each folder contains a different Lambda instance.
- I use SES and SNS for sending emails and processing them.
- There are 3 DynamoDB tables:
	1. For storing users and their preferences
	2. For storing competitions that have already been sent out
	3. Emails that cannot receive emails for whatever reason
