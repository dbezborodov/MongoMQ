# MongoMQ class #

Analogue of Amazon Simple Queue Service based on MongoDB

## Usage ##

    $mmq = new MongoMQ("mongodb://primary_server,secondary_server");
	if (!$mmq->sendMessage('test','message'))
		print $mmq->error;

# MongoMQ_SQS class #

MongoMQ extension, uses SQS as a backup service. Requires [SQS class](http://sourceforge.net/projects/php-sqs/).

## Usage ##
	$mmq = new MongoMQ_SQS(ACCESS_KEY,SECRET_ACCESS_KEY,"mongodb://primary_server,secondary_server");
 	$mmq->createQueue('test'); 
 	if (!$mmq->sendMessage('test','message'))
 		print $mmq->error;

