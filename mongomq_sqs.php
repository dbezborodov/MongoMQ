<?php
/**
 * MongoMQ extension with SQS backup
 * 
 * Usage:
 * <code>
 * $mmq = new MongoMQ_SQS(ACCESS_KEY,SECRET_ACCESS_KEY,"mongodb://primary_server,secondary_server");
 * $mmq->createQueue('test'); 
 * if (!$mmq->sendMessage('test','message'))
 *	print $mmq->error;
 * </code>
 *   
 * @author Dmitry Bezborodov
 * @version 0.1.0
 */

/**
 * Required MongoMQ class
 */
require_once("mongomq.php");
/**
 * Required Amazon SQS class
 * @link http://sourceforge.net/projects/php-sqs/
 * @author Dan Myers
 * @version 0.9.1
 */
require_once("sqs.php");

/**
 * MongoMQ extension with SQS backup
 *
 * Sends messages to SQS in case Mongo fails
 */
class MongoMQ_SQS extends MongoMQ
{
	/**
	 * @var string switched to 'sqs' if Mongo fails
	 */
	var $flag;
	/**
	 * @var string contains source of messages: 'mq' or 'sqs'
	 */
	var $source;

	/**
	 * Initialize Mongo and SQS
	 *
	 * @param string  $accessKey Access key
	 * @param string  $secretKey Secret key
	 * @param string  $server Mongo connection string ({@see http://us2.php.net/manual/en/mongo.construct.php})
	 * @param array   $options same as in Mongo class (optional)
	 * @param string  $dbname Mongo database which will be used to store queues (optional, 'queues' by default)
	 * @param integer $visibility_timeout Timeout to lock messages when reading, in seconds (optional, 3600 by default)
	 */			
	public function __construct ($accessKey, $secretKey, $server = "mongodb://localhost:27017", $options = array("connect" => TRUE), $dbname = 'queues', $visibility_timeout = 3600) {
	    $this->source = 'sqs';
	    try {
		$this->sqs = new SQS($accessKey, $secretKey);
        	parent::__construct($server,$options,$dbname,$visibility_timeout);
	    } catch(exception $e) {
		$this->__triggerError('__construct', $e->getMessage());
	    	$this->flag = 'sqs';
	    }
	}

	/**
	* Create a queue
	*
	* @param string  $queue The queue to create
	* @param integer $visibility_timeout The visibility timeout for the new queue
	* @return boolean
	*/
	public function createQueue($queue, $visibility_timeout = null) {
	    try {
		$this->sqs->createQueue($queue, $visibility_timeout);
		return true;
	    } catch(exception $e) {
		$this->__triggerError('createQueue', $e->getMessage());
		return false;
	    }
	}

	/**
	* Delete a queue
	*
	* @param string $queue The queue to delete
	* @return boolean
	*/
	public function deleteQueue($queue) {
	    try {
		$this->sqs->deleteQueue($queue);
		return parent::deleteQueue(queue);
	    } catch(exception $e) {
		$this->__triggerError('deleteQueue', $e->getMessage());
		return false;
	    }
	}

	/**
	* Get a queue's attributes
	*
	* @param string $queue The queue for which to retrieve attributes
	* @param string $attribute, Not used for compatibility with SQS class
	* @return array (name => value) | false
	*/
	public function getQueueAttributes($queue, $attribute = 'All') {
	    try {
		$result = $this->sqs->getQueueAttributes($queue, $attribute);
		$resultmq = parent::getQueueAttributes($queue, $attribute);
		$result['ApproximateNumberOfMessages'] += $resultmq['ApproximateNumberOfMessages'];
		$result['ApproximateNumberOfMessagesNotVisible'] += $resultmq['ApproximateNumberOfMessagesNotVisible'];
		$result['size'] = $resultmq['size'];
		return $result;
	    } catch(exception $e) {
		$this->__triggerError('getQueueAttributes', $e->getMessage());
		return false;
	    }
	}

	/**
	* Set attributes on SQS queue
	*
	* @param string $queue The queue for which to set attributes
	* @param string $attribute The name of the attribute to set
	* @param string $value The value of the attribute
	* @return boolean
	*/
	public function setQueueAttributes($queue, $attribute, $value) {
	    try {
		return $this->sqs->setQueueAttributes($queue, $attribute, $value);
	    } catch(exception $e) {
		$this->__triggerError('setQueueAttributes', $e->getMessage());
		return false;
	    }
	}

	/**
	* Send a message to a queue
	*
	* @param string $queue The queue which will receive the message
	* @param string $message The body of the message to send
	* @param array  $options Additional attributes for the message
	* @return boolean
	*/
	public function sendMessage($queue, $message, $options = array()) {
	    $max_errors = $options['max_errors']>0 ? $options['max_errors'] : 1;
	    if ($this->flag != 'sqs')
	    {
		$i = 0;
		do {
		    if (parent::sendMessage($queue, $message, $options))
			return true;
		    sleep(pow(2,++$i));
		} while ($i<$max_errors);

		$this->flag = 'sqs';
            }
        
	    if ($this->flag == 'sqs')
	    {
		try {
		    $i = 0;
		    do {
		    	if ($this->sqs->sendMessage($queue, $message))
			    return true;
		    	sleep(pow(2,++$i));
		    } while ($i<$max_errors);
		    $this->__triggerError('sendMessage', "Can't send a message to SQS");
                }
		catch(exception $e)
		{
		    $this->__triggerError('sendMessage', $e->getMessage());
        	}
        	return false;
	    }
	}

	/**
	* Receive a message from a queue
	*
	* @param string  $queue The queue for which to retrieve attributes
	* @param integer $num_messages The maximum number of messages to retrieve
	* @param integer $visibility_timeout The visibility timeout of the retrieved message
	* @return array of array(key => value) | false
	*/
	public function receiveMessage($queue, $num_messages = null, $visibility_timeout = null) {
	    // Receive messages from MongoMQ
	    if ($this->flag!='sqs' && $this->source!='sqs')
		return parent::receiveMessage($queue, $num_messages, $visibility_timeout);
	    else
	    {
		try {
	    	    $results = $this->sqs->receiveMessage($queue,$num_messages, $visibility_timeout);
	    	    if ($results===false) 
		    	$this->__triggerError('receiveMessage', "Can't receive message from SQS");
		}
		catch(exception $e)
		{
		    $this->__triggerError('receiveMessage', $e->getMessage());
		}

		if (sizeof($results)==0 && $this->flag!='sqs')
		{
			$this->source = 'mq';
			return parent::receiveMessage($queue, $num_messages, $visibility_timeout);
		}
	    	return $results;
	    }
	}

	/**
	* Delete a message from a queue
	*
	* @param string $queue The queue containing the message to delete
	* @param mixed $receipt_handle The id of the message to delete (MongoID) or ReceiptHandle for SQS or full message (array)
	* @return boolean
	*/
	public function deleteMessage($queue, $receipt_handle, $max_errors=1) {
            try
	    {
		$i = 0;
		if (is_array($receipt_handle))
		{
		    if ($receipt_handle['ReceiptHandle'])
			return $this->deleteMessage($queue, $receipt_handle['ReceiptHandle'], $max_errors);
		    elseif ($receipt_handle['_id'])
			return $this->deleteMessage($queue, $receipt_handle['_id'], $max_errors);
		    else
		    	$this->__triggerError('deleteMessage', "Don't know how to delete ".var_export($receipt_handle,1));
		}
		elseif (strlen($receipt_handle)<=32)
		{
		    $i = 0;
		    do {
		        if (parent::deleteMessage($queue,$receipt_handle))
			    return true;
			sleep(pow(2,++$i));
    		    } while ($i<$max_errors);
		    $this->__triggerError('deleteMessage', "Can't delete message from MongoMQ");
		}
		else
		{
	    	    $i = 0;
		    do {
		        if ($this->sqs->deleteMessage($queue,$receipt_handle))
		    	    return true;
			sleep(pow(2,++$i));
    		    } while ($i<$max_errors);
		    $this->__triggerError('deleteMessage', "Can't delete message from SQS");
		}
	    }
	    catch(exception $e)
	    {
		$this->__triggerError('deleteMessage', $e->getMessage());
    	    }
	    return false;
	}

} // class MongoMQ_SQS

?>