<?php
namespace Swoole\Coroutine\Pool;

class MySQLException extends \Exception {}

class MySQLPool
{
	static protected $init = false;

	static protected $spareConns = [];

	static protected $busyConns = [];

	static protected $connsConfig;

	static protected $connsNameMap = [];

	static protected $pendingFetchCount = [];

	static protected $resumeFetchCount = [];

	static public function init(array $connsConfig)
	{
		if (self::$init) {
			return;
		}
		self::$connsConfig = $connsConfig;
		foreach ($connsConfig as $name => $config) {
			self::$spareConns[$name] = [];
			self::$busyConns[$name] = [];
			self::$pendingFetchCount[$name] = 0;
			self::$resumeFetchCount[$name] = 0;
			if ($config['maxSpareConns'] <= 0 || $config['maxConns'] <= 0) {
				throw new MySQLException("Invalid maxSpareConns or maxConns in {$name}");
			}
		}
		self::$init = true;
	}

	static public function recycle(\Swoole\Coroutine\MySQL $conn)
	{
		if (!self::$init) {
			throw new MySQLException('Should call MySQLPool::init.');
		}

		$id = spl_object_hash($conn);
		$connName = self::$connsNameMap[$id];
		if (isset(self::$busyConns[$connName][$id])) {
			unset(self::$busyConns[$connName][$id]);
		} else {
			throw new MySQLException('Unknow MySQL connection.');
		}

		$connsPool = &self::$spareConns[$connName];
		if ($conn->connected) {
			if (count($connsPool) >= self::$connsConfig[$connName]['maxSpareConns']) {
				$conn->close();
			} else {
				$connsPool[] = $conn;
				if (self::$pendingFetchCount[$connName] > 0) {
					self::$resumeFetchCount[$connName]++;
					self::$pendingFetchCount[$connName]--;
					\Swoole\Coroutine::resume('MySQLPool::' . $connName);
				}
				return;
			}
		}

		unset(self::$connsNameMap[$id]);
	}

	static public function fetch($connName)
	{
		if (!self::$init) {
			throw new MySQLException('Should call MySQLPool::init!');
		}

		if (!isset(self::$connsConfig[$connName])) {
			throw new MySQLException("Unvalid connName: {$connName}.");
		}

		$connsPool = &self::$spareConns[$connName];
		if (!empty($connsPool) && count($connsPool) > self::$resumeFetchCount[$connName]) {
			$conn = array_pop($connsPool);
			if ($conn->connected) {
				self::$busyConns[$connName][spl_object_hash($conn)] = $conn;
				return $conn;
			}
		}

		if (count(self::$busyConns[$connName]) + count($connsPool) == self::$connsConfig[$connName]['maxConns']) {
			self::$pendingFetchCount[$connName]++;
			if (\Swoole\Coroutine::suspend('MySQLPool::' . $connName) == false) {
				self::$pendingFetchCount[$connName]--;
				throw new MySQLException('Reach max connections! Cann\'t pending fetch!');
			}
			self::$resumeFetchCount[$connName]--;
			if (!empty($connsPool)) {
				$conn = array_pop($connsPool);
				if ($conn->connected) {
					self::$busyConns[$connName][spl_object_hash($conn)] = $conn;
					return $conn;
				}
			} else {
				return false;//should not happen
			}
		}

		$conn = new \Swoole\Coroutine\MySQL();
		$id = spl_object_hash($conn);
		self::$connsNameMap[$id] = $connName;
		self::$busyConns[$connName][$id] = $conn;
		if ($conn->connect(self::$connsConfig[$connName]['serverInfo']) == false) {
			unset(self::$busyConns[$connName][$id]);
			unset(self::$connsNameMap[$id]);
			throw new MySQLException('Cann\'t connect to MySQL server: ' . json_encode(self::$connsConfig[$connName]['serverInfo']));
		}

		return $conn;
	}
}
