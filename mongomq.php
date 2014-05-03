<?php
/**
* Mongo MQ PHP class
*
* Amazon SQS analogue based on MongoDB ({@link http://www.mongodb.org/})
* Usage:
* <code>
*   $mmq = new MongoMQ("mongodb://primary_server,secondary_server");
*   if (!$mmq->sendMessage('test','message'))
*	print $mmq->error;
* </code>

* @author Dmitry Bezborodov
* @version 0.1.0
*/
class MongoMQ extends MongoClient
{
	/*
	 * contains last MongoMQ error
	 * @access public
	 * @var string 
	 */
	public $error = '';
	/*
	 * @property string name of Mongo database which will be used to store queues
	 */
	private  $dbname='';
	/*
	 * @property integer Timeout in seconds used in receiveMessage() to lock messages
	 */
	private  $visibility_timeout=3600;

	/**
	* Initialize MongoMQ
	*
	* @param string  $server Mongo connection string ({@see http://us2.php.net/manual/en/mongo.construct.php})
	* @param array   $options same as in Mongo class (optional)
	* @param string  $dbname Mongo database which will be used to store queues (optional, 'queues' by default)
	* @param integer $visibility_timeout Timeout to lock messages when reading, in seconds (optional, 3600 by default)
	*/
	public function __construct ($server = "mongodb://localhost:27017", $options = array("connect" => TRUE), $dbname = 'queues', $visibility_timeout = 3600) {
        	parent::__construct($server,$options);
	
		$this->dbname = $dbname;
		$this->visibility_timeout = $visibility_timeout;
	}

	/**
	* Create a queue
	*
	* @param string  $queue The queue to create
	* @param integer $visibility_timeout Not used, for compatibility with SQS class
	* @return boolean
	*/
	public function createQueue($queue, $visibility_timeout = null) {
	    try {
		return is_object($this->selectDB($this->dbname)->createCollection($queue));
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
		$response = $this->selectDB($this->dbname)->selectCollection($queue)->drop();
		return $response['ok'];
	    } catch(exception $e) {
		$this->__triggerError('deleteQueue', $e->getMessage());
		return false;
	    }
	}

	/**
	* Get a list of queues
	*
	* @param string $prefix Only return queues starting with this string (optional)
	* @return array | false
	*/
	public function listQueues($prefix = '') {
	    try {
		$list = $this->selectDB($this->dbname)->listCollections();
		$result = array();
		foreach ($list as $collection) {
		    $name = $collection->getName();
		    if (!$prefix || strpos($name,$prefix)===0)
			$result[] = $name;
		}
		return $result;
	    } catch(exception $e) {
		$this->__triggerError('listQueues', $e->getMessage());
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
		$total = $this->selectDB($this->dbname)->selectCollection($queue)->count();
		$new = $this->selectDB($this->dbname)->selectCollection($queue)->count(array('started' => 0));
		return array(
			'size' => $total,
			'ApproximateNumberOfMessages' => $new,
			'ApproximateNumberOfMessagesNotVisible' => $total-$new
			);
	    } catch(exception $e) {
		$this->__triggerError('getQueueAttributes', $e->getMessage());
		return false;
	    }
	}

	/**
	* Set attributes on a queue
	*
	* Not used, for compatibility with SQS class
	*/
	public function setQueueAttributes($queue, $attribute, $value) {
		return true;
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
	    $item = array(  'Message' => $message,
				'started' => 0,
				'timestamp' => time()
			);
	    foreach ($options as $name => $value)
		if (!$item[$name])
		    $item[$name] = $value;
	    try {
	    	$this->selectDB($this->dbname)->selectCollection($queue)->insert($item, array('safe' => true));
	    } catch(exception $e) {
		$this->__triggerError('sendMessage', $e->getMessage());
		return false;
	    }
	    return true;
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
	    if (!$num_messages) $num_messages = 1;
	    if (!$visibility_timeout) $visibility_timeout = $this->visibility_timeout;

	    $results = array();
	    for ($i=0; $i<$num_messages; $i++)
	    {
	    	try {
	    	    $this->selectDB($this->dbname)->selectCollection($this->queue)->ensureIndex( array("started" => 1), array('background' => true) );
	
		    $result = $this->selectDB($this->dbname)->selectCollection('$cmd')->find( array('findAndModify' => $queue, 
								 'query' => array( 'started' => array('$lt'  => time()-$visibility_timeout)), 
								 'update' => array('$set' => array('started' => time())) 
								) )->limit(-1)->timeout(300000)->getNext(); 

		    if ($result['ok'])
		    {
			$result['value']['Body'] = $result['value']['Message'];
			$results[] = $result['value'];
		    }
		    elseif (!strstr($result['errmsg'],'No matching object'))
		    {
			$this->__triggerError('receiveMessage', $result['errmsg']);
			return false;
		    }
		    else
			break;
	    	} catch(exception $e) {
		    $this->__triggerError('receiveMessage', $e->getMessage());
		    return false;
    	    	}
	    }
	    return $results;
	}

	/**
	* Delete a message from a queue
	*
	* @param string $queue The queue containing the message to delete
	* @param string $receipt_handle The id of the message to delete (MongoID)
	* @return boolean
	*/
	public function deleteMessage($queue, $receipt_handle) {
            try {
		if (!is_object($receipt_handle)) $receipt_handle = new MongoId($receipt_handle);
	    	$result = $this->selectDB($this->dbname)->selectCollection($queue)->remove(array('_id' => $receipt_handle),array('safe' => true));
		if ($result['ok'])
		{
		    if ($result['n']>0)
		    	return true;
		    else
		    {
	    	    	$this->__triggerError('deleteMessage', "Wrong ID, no messages deleted");
		    	return false;
		    }
		}
		else
		{
	    	    $this->__triggerError('deleteMessage', $result['err']);
		    return false;
		}
	    } catch(exception $e) {
	    	$this->__triggerError('deleteMessage', $e->getMessage());
		return false;
    	    }
	}

	/**
	* Save an error message
	*
	* @internal Used by member functions to output errors
	* @param string $error Error message
	* @return none
	*/
	public function __triggerError($functionname, $error)
	{
		$this->error = sprintf("SQS::%s(): error %s", $functionname, $error);
	}
}

?>