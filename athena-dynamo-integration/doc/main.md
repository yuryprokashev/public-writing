# How to query DynamoDB using SQL?
Alright, so you have a DynamoDB table with some data, and you want to query it with SQL?

That's cool! Bear with me, and we will set it up in a few minutes!

The trick is to use Athena SQL, but before using it we need to connect Athena with DynamoDB.

## Creating a DynamoDB data source in Athena
### Where to start?
In AWS Console go to Athena service
- Expand the left side panel 
- Click on "Data sources"
- Click "Add data source" button

![](query%20dynamo%20data%20with%20athena.%2001.png)

### Configuring the data source
- Select "Amazon DynamoDB" as the data source type and click `Next` button.

![](query%20dynamo%20data%20with%20athena.%2002.png)

- Enter Data source name.

> Note: the field tip is confusing. 
   At the end, your datasource will contain all DynamoDB tables in the region, so choose wisely. 
   I ended up with `DynamoDBCatalog`.

![](query%20dynamo%20data%20with%20athena.%2003.png)

- Click `Create Lambda function`.
### Creating the connector Lambda function
You will be redirected to another page.

There are two important properties to set here:
- `SpillBucket` - a bucket where Athena will store query results. This bucket should exist, so create it if you don't have one.
- `AthenaCatalogName` - the field name is confusing, but the field tip is right - it is in fact the name of 
the lambda function that will translate your Athena SQL to DynamoDB queries. 
I ended with `dynamo-connector-for-athena`.

- Click `Create Application` when done.
![](query%20dynamo%20data%20with%20athena.%2004.png)

### Finalizing your data source
After your connector lambda is created, 
- Go back to you Data source creation page, 
- Refresh the list of lambda functions
- Pick the one you created.
- Click `Next`.
![](query%20dynamo%20data%20with%20athena.%2005.png)
Click `Review and create`.
![](query%20dynamo%20data%20with%20athena.%2006.png)
### Querying the data
Now let's query our DynamoDB table using Athena SQL.
- Open Athena Query Editor.
- Change the Data source to the one you created.
- Write an SQL query for a Dynamo DB table from the `Table` list.
- Run it! :tada:

![](query%20dynamo%20data%20with%20athena.%2008.png)
