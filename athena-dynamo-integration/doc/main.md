# How to query DynamoDB using SQL?

Alright, so you have a DynamoDB table with some data, and you want to query it with SQL?

That's cool! Bear with me, and we will set it up in a few minutes!

The trick is to use Athena SQL, but before using it we need to connect Athena with DynamoDB.

## Creating a DynamoDB data source in Athena

### Where to start?

In AWS Console go to Athena service

* Expand the left side panel
* Click on the "Data sources"
* Click the "Add data source" button

![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._01_2IGU3fXlf.png "Create Athena Data Source")

### Configuring the data source

* Select "Amazon DynamoDB" as the data source type and click the `Next` button.

![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._02_DbYbtw7zm.png "Select Data source type")

* Enter the Data source name.

> Note: the field tip is confusing.
> At the end, your data source will contain all DynamoDB tables in the region, so choose wisely.
> I ended up with `DynamoDBCatalog`.

![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._03_BpEHtmyx8.png "Configure Data source properties")

* Click `Create Lambda function`. You see another page in a new tab.

### Creating the connector Lambda function

There are two important properties to set here:

* `SpillBucket` - a bucket where Athena will store query results. This bucket should exist, so create it if you don't have one.
* `AthenaCatalogName` - the field name is confusing, but the field tip is right - it is the name of the lambda function that will translate your Athena SQL to DynamoDB queries. I ended with `dynamo-connector-for-athena`.
* Click the `Create Application` button when done.
  ![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._04_MoNWKSZxB.png "Create connector lambda function")

### Finalizing your data source

After your connector lambda is created,

* Go back to your Data source creation page,
* Refresh the list of lambda functions
* Pick the one you created.
* Click `Next`.
  ![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._05_rcxnv_k1J.png "Pick the connector lambda")
  Click `Review and create`.
  ![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._06_FqeIA4hRE.png "Review and create")

## Querying the data

Now let's query our DynamoDB table using Athena SQL.

* Open Athena Query Editor.
* Change the Data source to the one you created.
* Write an SQL query for a Dynamo DB table from the `Table` list.
* Run it!

![](https://ik.imagekit.io/wp8orxehk/differ/community/yury-prokashev-edufun-me/query_dynamo_data_with_athena._08_8C9MQE5yV.png "Run it")