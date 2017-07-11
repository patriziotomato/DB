<?php
namespace ScoopDb;

/*
 * DB ORM Class for easy DB access.
 *
 * @author Patrick Thomas <patrick.thomas@staudacher.de>
 *
 * @version 1.4     DBTable eingefuehrt
 * @version 1.3.1   Klasse neu formatiert
 * @version 1.3     DBSelect Objekt hinzugefuegt (Aus scoop_base uebernommen)
 */

define('COLUMN_FIELD_SET_STRING', 1);
define('COLUMN_TYPE_VARCHAR', MYSQLI_TYPE_VAR_STRING);
define('COLUMN_TYPE_CHAR', MYSQLI_TYPE_STRING);
define('COLUMN_TYPE_ENUM', MYSQLI_TYPE_ENUM);
define('COLUMN_TYPE_SET', MYSQLI_TYPE_SET);
define('COLUMN_TYPE_TINYBLOB', MYSQLI_TYPE_TINY_BLOB);
define('COLUMN_TYPE_MEDIUMBLOB', MYSQLI_TYPE_MEDIUM_BLOB);
define('COLUMN_TYPE_LONGBLOB', MYSQLI_TYPE_LONG_BLOB);
define('COLUMN_TYPE_BLOB', MYSQLI_TYPE_BLOB);
define('COLUMN_TYPE_DATE', MYSQLI_TYPE_DATE);
define('COLUMN_TYPE_DATETIME', MYSQLI_TYPE_DATETIME);
define('COLUMN_TYPE_TIME', MYSQLI_TYPE_TIME);
define('COLUMN_TYPE_TIMESTAMP', MYSQLI_TYPE_TIMESTAMP);

define('COLUMN_FIELD_SET_NUMERIC', 2);
define('COLUMN_TYPE_DECIMAL', MYSQLI_TYPE_DECIMAL);
define('COLUMN_TYPE_INT_TINY', MYSQLI_TYPE_TINY);
define('COLUMN_TYPE_INT_SMALL', MYSQLI_TYPE_SHORT);
define('COLUMN_TYPE_INT', MYSQLI_TYPE_LONG);
define('COLUMN_TYPE_FLOAT', MYSQLI_TYPE_FLOAT);
define('COLUMN_TYPE_DOUBLE', MYSQLI_TYPE_DOUBLE);
// too lazy now....

// Some funny bits you can use whereever you want/need
define('COLUMN_FOR_INSERT_ONLY', 1 << 1);
define('COLUMN_FOR_UPDATE_ONLY', 1 << 2);

class DB
{
    public $con; // Holds DB connection
    protected $db;
    protected $host;
    protected $user;
    protected $debugCounter = 0;

    /**
     * @var $dumpMethod \Closure This Closure Method is responsible for dumping an information. Can be overwritten in your project.
     * This Closure gets a string to dump do not return any value. It is fully responsible to print the desired
     * information!
     */
    protected $dumpMethod;

    protected static $_fieldInfoCache = array();

    protected static $connectionCache = array();

    /**
     * @param null $host
     * @param null $user
     * @param null $pwd
     * @param null $db
     * @param null $dumpMethod
     * @return DB $db
     */
    public static function get($host = null, $user = null, $pwd = null, $db = null, $dumpMethod = null)
    {
        if (!$host) {
            $host = DB_HOST;
        }
        if (!$user) {
            $user = DB_USER;
        }
        if (!$pwd) {
            $pwd = DB_PWD;
        }
        if (!$db) {
            $db = DB_DATABASE;
        }

        if (!static::$connectionCache[$host][$user][$pwd][$db]) {
            static::$connectionCache[$host][$user][$pwd][$db] = new DB($host, $user, $pwd, $db, $dumpMethod);
        }

        return static::$connectionCache[$host][$user][$pwd][$db];
    }

    public function __construct($host, $user, $pwd, $db, $dumpMethod = null)
    {
        $this->db = $db;
        $this->host = $host;
        $this->user = $user;
        $this->con = new \mysqli($host, $user, $pwd, $db);
        if ($this->con->connect_errno) {
            throw new \Exception("Failed to connect to MySQL: " . $this->con->connect_error);
        }
        $this->con->set_charset("utf8");

        $this->setDumpMethod($dumpMethod);
    }



    public function debug($debugCounter = 1)
    {
        $this->debugCounter = $debugCounter;
    }

    /**
     * @param $sql
     * @return bool|\mysqli_result
     */
    public function query($sql)
    {
        if ($this->debugCounter-- > 0) {
            $this->dumpMethod->__invoke($sql);
        }
        $ret = $this->con->query($sql);

        if (!$ret) {
            $ref = array();
            $stacktrace = debug_backtrace();
            foreach ($stacktrace AS $stack) {
                $ref[] = basename($stack['file']) . ':' . $stack['line'];
            }

            $caller = implode(' > ', array_reverse($ref));
            $error = $this->con->error;
            print '<pre><h3><font color="FF0000">SQL Fehler!<br>' . $error . '</font><br>' . $sql . '<br><font color="FF0000">' . $caller . '</font></h3></pre>';

            $oneLineInfo = str_replace("\n", "", $sql . ' stacktrace: ' . $caller);
            $oneLineInfo = str_replace("\t", " ", $oneLineInfo);

            trigger_error("SQL Fehler: $error: $oneLineInfo\nSQL: {$sql}", E_USER_WARNING);
            exit;
        }

        return $ret;
    }

    /*
     * $res = $db->queryMulti($sql);
     * do {
     *    if ($res = $db->store_result()) {
     *        $this->dumpMethod->__invoke($res->fetch_all(MYSQLI_ASSOC));
     *        $res->free();
     *    }
     * } while ($db->more_results() && $db->next_result());
     *
     */
    public function queryMulti($sql)
    {
        if (is_array($sql)) { // you can pass several statements also packed within an array
            $sql = implode('; ', $sql);
        }

        if ($this->debugCounter-- > 0) {
            $this->dumpMethod->__invoke($sql);
        }
        $ret = $this->con->multi_query($sql);

        if (!$ret) {
            $ref = array();
            $stacktrace = debug_backtrace();
            foreach ($stacktrace AS $stack) {
                $ref[] = basename($stack['file']) . ':' . $stack['line'];
            }

            $caller = implode(' > ', array_reverse($ref));
            $error = $this->con->error;
            print '<pre><h3><font color="FF0000">SQL Fehler!<br>' . $error . '</font><br>' . $sql . '<br><font color="FF0000">' . $caller . '</font></h3></pre>';

            $oneLineInfo = str_replace("\n", "", $sql . ' stacktrace: ' . $caller);
            $oneLineInfo = str_replace("\t", " ", $oneLineInfo);

            trigger_error("SQL Fehler: $error: $oneLineInfo", E_USER_WARNING);
            exit;
        }

        // Finally get rid of this funny Error: Commands out of sync; you can't run this command now!!
        // DAMN IT! It's been the multi_query
        while ($this->con->more_results() && $this->con->next_result()) {
            $extraResult = $this->con->use_result();
            if ($extraResult instanceof \mysqli_result) {
                $extraResult->free();
            }
        }

        return $ret;
    }

    public function queryInsert($sql)
    {
        if ($this->query($sql)) {
            return $this->con->insert_id;
        } else {
            return false;
        }
    }

    public function affectedRows()
    {
        return $this->con->affected_rows;
    }

    public function queryAndFetchAllSelect(DBSelect $select, $field4ArrayKey = null, $ret = null)
    {
        return $this->queryAndFetchAll($field4ArrayKey, $select->sql(), $ret);
    }

    public function queryAndFetchAll($field4ArrayKey, $sql, $ret = null)
    {
        if (!$ret) {
            $ret = array();
        }
        $result = $this->query($sql);
        if ($result) {
            while ($tmp = $result->fetch_array(MYSQLI_ASSOC)) {
                if (is_array($field4ArrayKey) && 2 == count($field4ArrayKey)) {
                    $ret[$tmp[$field4ArrayKey[0]]][$tmp[$field4ArrayKey[1]]] = $tmp;
                } else {
                    if (is_array($field4ArrayKey) && 3 == count($field4ArrayKey)) {
                        $ret[$tmp[$field4ArrayKey[0]]][$tmp[$field4ArrayKey[1]]][$tmp[$field4ArrayKey[2]]] = $tmp;
                    } else {
                        if ($field4ArrayKey) {
                            $ret[$tmp[$field4ArrayKey]] = $tmp;
                        } else {
                            $ret[] = $tmp;
                        }
                    }
                }
            }
            $result->free();
            #$result->close();
            return $ret;
        } else {
            return $result;
        }
    }

    public function queryAndFetchSingleColumnSelect(DBSelect $select, $column = null)
    {
        return $this->queryAndFetchSingleColumn($select->sql(), $column);
    }

    public function queryAndFetchSingleColumn($sql, $column = null)
    {
        $selectFunction = MYSQLI_ASSOC;
        if (!$column) {
            $selectFunction = MYSQLI_NUM;
            $column = 0;
        }

        $result = $this->query($sql);
        if ($result) {
            for ($ret = array(); $row = $result->fetch_array($selectFunction);) {
                $ret[] = $row[$column];
            }

            return $ret;
        } else {
            return $result;
        }
    }

    /**
     * @deprecated
     */
    function querySingleValueSelect(DBSelect $select, $columnName = null)
    {
        return $this->querySingleValue($select->sql(), $columnName);
    }

    function querySingleValue($query, $columnName = null)
    {
        if (!$columnName) {
            $rs = $this->query($query)->fetch_array(MYSQLI_NUM);
        } else {
            $rs = $this->query($query)->fetch_array(MYSQLI_ASSOC);
        }

        return $rs[$columnName ? $columnName : 0];
    }

    /**
     * Just a wrapper for #queryAndFetchArray to specify a DBSelect object to be executed
     *
     * @param DBSelect $select
     * @return bool|mixed|mysqli_result
     */
    function queryAndFetchArraySelect(DBSelect $select)
    {
        return $this->queryAndFetchArray($select->sql());
    }

    /**
     * Fetches a single line as assotiative array
     *
     * @param $sql
     * @return bool|mixed|\mysqli_result
     */
    public function queryAndFetchArray($sql)
    {
        $result = $this->query($sql);
        if ($result) {
            return $result->fetch_array(MYSQLI_ASSOC);
        } else {
            return $result;
        }
    }

    /**
     * @param DBSelect $select
     * @return int
     */
    public function countSelect(DBSelect $select)
    {
        $select->columnsToSelect(array('COUNT(*)'));

//        $this->querySingleValueSelect($select);
//        exit;
        return $this->querySingleValueSelect($select);
    }

    /**
     * Modifies any DBSelect to delete entries of the "main" table ($db->select('mainTable')) or any other $tableAliases
     *
     * @param DBSelect $select
     * @param string|array $tableAliases One or many tableAliases
     * @param bool $isTestMode
     * @return bool|mixed|mysqli_result
     */
    public function delete(DBSelect $select, $tableAliases = null, $isTestMode = false)
    {
        // Convert the select to a delete statement
        $select->columnsToSelect(array());

        $tableToDeleteAliases = array();
        if (!$tableAliases) {
            $tableToDeleteAliases[] = $select->_table;
        } else if (!is_array($tableAliases)) {
            $tableToDeleteAliases[] = $tableAliases;
        } else { // An array of tables has been specified
            $tableToDeleteAliases = $tableAliases;
        }

        $sqlDelete = str_replace('SELECT *', 'DELETE ' . implode(', ', $tableToDeleteAliases), $select->sql());

        if ($isTestMode) {
            return $sqlDelete;
        } else {
            return $this->query($sqlDelete);
        }
    }

    public function escape($string)
    {
        return $this->con->real_escape_string($string);
    }

    /**
     * @param $table
     * @param $pkId
     * @param string $pkColumnName
     * @return DBUpdate
     * @throws Exception
     */
    public function prepareUpdate($table, $pkId, $pkColumnName = 'id')
    {
        if (!$pkId) {
            throw new \Exception("Cannot prepare Update of table $table with pkId #$pkId");
        }
        return DBUpdate::getSinglePKColumn($this, $table, $pkId, $pkColumnName);
    }

    /**
     * @param $table
     * @param string $pkColumnName
     * @return DBUpdate
     * @deprecated User #prepareInsert as this method automatically detects the autoincrement columns
     */
    public function prepareInsertWithoutPK($table, $pkColumnName = 'id')
    {
        // Determine the next free PK-ID (because we cannot push a NULL to this column easily right now)
        $nextPK = $this->querySingleValue("SELECT MAX(`$pkColumnName`) + 1 FROM $table");

        return DBUpdate::getSinglePKColumn($this, $table, $nextPK, $pkColumnName);
    }

    public function prepareInsert($table) {
        return new DBUpdate($this, $table, null);
    }

    public static function checkForFieldset($mysqliColumnInfo, $fieldSet)
    {
        $fieldSets = array(
            COLUMN_FIELD_SET_STRING  => array(
                COLUMN_TYPE_VARCHAR,
                COLUMN_TYPE_CHAR,
                COLUMN_TYPE_ENUM,
                COLUMN_TYPE_SET,
                COLUMN_TYPE_TINYBLOB,
                COLUMN_TYPE_MEDIUMBLOB,
                COLUMN_TYPE_LONGBLOB,
                COLUMN_TYPE_BLOB,
                COLUMN_TYPE_DATE,
                COLUMN_TYPE_DATETIME,
                COLUMN_TYPE_TIME,
                COLUMN_TYPE_TIMESTAMP // A date have to be passed
            ),
            COLUMN_FIELD_SET_NUMERIC => array(
                COLUMN_TYPE_DECIMAL,
                COLUMN_TYPE_INT_TINY,
                COLUMN_TYPE_INT_SMALL,
                COLUMN_TYPE_INT,
                COLUMN_TYPE_FLOAT,
                COLUMN_TYPE_DOUBLE
            )
        );

        return in_array($mysqliColumnInfo->type, $fieldSets[$fieldSet]);
    }

    public static function checkCanBeNull($mysqliColumnInfo)
    {
        return !(MYSQLI_NOT_NULL_FLAG & $mysqliColumnInfo->flags);
    }

    public static function checkIsPK($mysqliColumnInfo)
    {
        return (bool)(MYSQLI_PRI_KEY_FLAG & $mysqliColumnInfo->flags);
    }

    public static function checkIsAutoIncrement($mysqliColumnInfo)
    {
        return (bool)(MYSQLI_AUTO_INCREMENT_FLAG & $mysqliColumnInfo->flags);
    }

    /**
     * @deprecated Use #mask($column, $value) instead!
     */
    public function maskNewColumnValue($columnName, $value, $fieldInfo)
    {
        // TODO: Falls das Feld NULL sein kann, und $value NULL ist, dann auf NULL setzen
        if (self::checkCanBeNull($fieldInfo) && $value === null) {
            return "NULL";
        } else {
            if (self::detectMySQLFunction($value)) {
                return $value; // !! If you use a mysql function to insert a value, you need to take care on masking/escaping by yourself!!
            } else {
                if (self::checkForFieldset($fieldInfo, COLUMN_FIELD_SET_STRING)) { // Is that a text column?
                    return "'" . $this->escape($value) . "'";
                } else {
                    return $this->escape($value);
                }
            }
        }
    }


    // Replacing
    public function mask($columnInfo, $value)
    {
        // Maybe not the best place for this conversion!
        if (in_array($columnInfo->type, array(COLUMN_TYPE_FLOAT, COLUMN_TYPE_DOUBLE))) {
            $value = str_replace(',', '.', $value);
        }
        // Falls das Feld NULL sein kann, und $value NULL ist, dann auf NULL setzen
        if (self::checkCanBeNull($columnInfo) && $value === null) {
            return "NULL";
        } else if (self::detectMySQLFunction($value)) {
            return $value; // !! If you use a mysql function to insert a value, you need to take care on masking/escaping by yourself!!
        } else if (self::checkForFieldset($columnInfo, COLUMN_FIELD_SET_STRING)) { // Is that a text column?
            return "'" . $this->escape($value) . "'";
        } else if (self::checkForFieldset(
                $columnInfo,
                COLUMN_FIELD_SET_NUMERIC
            ) && !$value
        ) { // Is that a number column, without any value?
            return 0;
        } else {
            return $this->escape($value);
        }
    }

    public function performFullDataBaseBackup($targetDumpFile)
    {
        if (file_exists($targetDumpFile)) {
            unlink($targetDumpFile);
        }
        if (file_exists($targetDumpFile . '.zip')) {
            unlink($targetDumpFile . '.zip');
        }

        // Use mysqldump to fastly perform a full database dump
        shell_exec(
            "mysqldump -h " . DB_HOST . " --user " . DB_USER . " --password=" . DB_PWD . " " . DB_DATABASE . " > $targetDumpFile"
        );

        $zip = new \ZipArchive();
        if ($zip->open($targetDumpFile . '.zip', \ZipArchive::CREATE) !== true) {
            die("cannot open {$targetDumpFile}.zip>\n");
        }

        $zipOk = $zip->addFile($targetDumpFile, basename($targetDumpFile));
        $zip->close();

        if ($zipOk) {
            unlink($targetDumpFile);
        }
    }

    public static function detectMySQLFunction($value)
    {
        switch (true) {
            case $value instanceof DBExpression:
            case ('NOW()' == $value):
                return true;
            // case ('SUBSTR(' == substr($value, 0, 7)): return true;
        }
        return false;
    }

    public function getAllTables(\closure $operation = null)
    {
        return $this->queryAndFetchSingleColumn("SHOW TABLES FROM {$this->db}", "Tables_in_{$this->db}", $operation);
    }

    public function select($table)
    {
        return new DBSelect($this, $table);
    }

    /**
     * Deletes all rows of a specified table. Please be careful using this, as all rows are gone after execution!
     *
     * @param String $table The table name to be truncated
     * @return bool|\mysqli_result
     */
    public function truncateTable($table) {
        $dbTable = new DBTable($this, $table);
        return $dbTable->truncate();
    }

    public function insertFromSelect(DBSelect $select, $tableToInsert, $targetColumns, $testMode = false)
    {
        $sql = "INSERT INTO {$tableToInsert} ";
        $sql .= "(" . implode(', ', $targetColumns) . ")\n";
        $sql .= $select->sql();

        if ($testMode) {
            return $sql;
        } else {
            return $this->query($sql);
        }
    }

    public function insertFromSelectOrUpdate(
        DBSelect $select,
        $tableToInsert,
        $columnsToInsert,
        $ignoreDuplicateKeyErrors = false,
        $testMode = false
    ) {
        $sourceColumns = array();
        $targetColumns = array();
        $onDuplicateParts = array();

        $i = 0;
        foreach ($columnsToInsert AS $columnPair) {
            $sourceColumn = key($columnPair);
            $targetColumn = $columnPair[$sourceColumn];

            array_shift($columnPair);
            $columnOptions = array_shift($columnPair);

            $sourceColumns[] = $sourceColumn;
            $targetColumns[] = $targetColumn;

            if (!(COLUMN_FOR_INSERT_ONLY & $columnOptions)) {
                $onDuplicateParts[] = "{$targetColumn} = {$sourceColumn}";
            }

            $i++;
        }
        $targetColumnsSQL = implode(', ', $targetColumns);

        $sql = '';
        if ($ignoreDuplicateKeyErrors) {
            $sql .= "INSERT IGNORE INTO ";
        } else {
            $sql .= "INSERT INTO ";
        }

        $sql .= "{$tableToInsert} ($targetColumnsSQL)\n";
        $sql .= $select->columnsToSelect($sourceColumns)->sql();

        if (count($onDuplicateParts) > 0) {
            $sql .= "\nON DUPLICATE KEY UPDATE\n";
            $sql .= implode(', ', $onDuplicateParts);
        }

        if ($testMode) {
            return $sql;
        } else {
            return $this->query($sql);
        }
    }

    /**
     * You're responsible to delete this tmp file again!
     *
     * @param $tableName
     * @param $localFileName
     * @param array $options
     *      - addColumnHeaders = [true|false] => Prepends the column headers to the csv file
     * @throws Exception
     * @return bool|string
     */
    public function writeCSVOfTable($tableName, $localFileName, $options = array())
    {
        if (file_exists($localFileName)) {
            unlink($localFileName);
        }

        if (!($lineending = $options['lineending'])) {
            $lineending = '\\r\\n';
        }

        if (!($escape = $options['escape'])) {
            $escape = '"';
        }

        if (!array_key_exists('orderby', $options)) {
            $options['orderby'] = 'ORDER BY ArtikelNr';
        }

        if (!array_key_exists('enclosing', $options)) {
            $options['enclosing'] = '\\"';
        }

        $columns = '*';
        if (is_array($options['columns'])) {
            $columns = implode(', ', $options['columns']);
        }


        $columnHeadersSQLPart = '';
        if ($options['addColumnHeaders']) {

            if ($options['orderby']) {
                // the column headers would get sorted like any other row using the method below! if you need this,
                // write a more rock solid code!
                throw new Exception("Cannot sort CSV while using column headers");
            }

            $columnLimitSQLPart = '';
            if (is_array($options['columns'])) {
                $columnLimitSQLPart = " AND COLUMN_NAME IN ('" . implode("', '", $options['columns']) . "')";
            }

            $sqlColumnHeaders = "select GROUP_CONCAT(CONCAT(\"'\",COLUMN_NAME,\"'\"))
                from INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{$tableName}'
                AND TABLE_SCHEMA = '{$this->db}'
                {$columnLimitSQLPart}
            ";

            $columnHeadersSQLPart = "SELECT " . $this->querySingleValue($sqlColumnHeaders) . " UNION ALL ";
        }

        $sql = "
            $columnHeadersSQLPart
            SELECT {$columns} INTO OUTFILE '{$localFileName}'
            FIELDS TERMINATED BY ';'
            ENCLOSED BY '{$options['enclosing']}'
            ESCAPED BY '" . $escape . "'
            LINES TERMINATED BY '" . $lineending . "'
            FROM {$tableName}
            {$options[orderby]}
        ";

        return $this->query($sql);
    }

    /**
     * This copies a set of tables, to an other database.
     *
     * - tables won't be dropped upfront. Only a CREATE TABLE comes to the target DB
     * - You can rename tables by specifying an associative array in $tables-parameter
     * - For reading data, mysqldump will be used
     * - ... to pipe the data directly to mysql command
     *
     * @param $tables array Either array('table1', 'table2', ...) or if you want to translate table names array('tableFrom' => 'tableTo', ...)
     * @param $targetDB DB Target DB object
     * @param $sourceDBPassword String Needed, not to store the passwords within the object
     * @param $targetDBPassword String Needed, not to store the passwords within the object
     * @return String Error string (should be empty if all went well!)
     */
    public function copyTablesToNewDB($tables, $targetDB, $sourceDBPassword, $targetDBPassword)
    {
        $error = '';

        $allTables = array(); // A sourceTable can be empty if table names should not be translated
        foreach ($tables AS $sourceTable => $targetTable) {
            if (!$sourceTable) {
                $sourceTable = $targetTable;
            }
            $allTables[$sourceTable] = $targetTable;
        }

        // Build the shell command
        $cmd = "mysqldump -h {$this->host} -u {$this->user} -p{$sourceDBPassword}";
        $cmd .= " --no-create-db --skip-add-drop-table";
        $cmd .= " {$this->db} " . implode(' ', array_keys($allTables));

        // Now pipe to SED to translated old to new table name
        foreach ($allTables AS $sourceTable => $targetTable) {
            if ($sourceTable != $targetTable) {
                $cmd .= " | sed -e 's/`{$sourceTable}`/`{$targetTable}`/'";
            }
        }

        $cmd .= " | mysql -h {$targetDB->host} -u {$targetDB->user} -p{$targetDBPassword} {$targetDB->db}";

        // PAT: Auftretende Fehler werden irgendwie nicht immer sauber zurückgeliefert. Sollte mal auf proc_open
        // umgestellt werden
        $fp = popen($cmd . ' 2>&1', "r");
        while (!feof($fp)) {
            $error .= fgets($fp, 4096);
        }
        pclose($fp);

        return trim($error);
    }

    public function fieldInfoColumn($table, $column, $alias = null)
    {
        $fieldInfo = $this->fieldInfo($table, $alias);
        return $fieldInfo[$column];
    }

    public function fieldInfo($table, $alias = null)
    {
        if (!$alias) {
            $alias = $table;
        }

        if (!self::$_fieldInfoCache[$alias]) {
            $result = $this->query("SELECT * FROM $table LIMIT 1");
            while ($fieldInfo = $result->fetch_field()) {
                self::$_fieldInfoCache[$alias][$fieldInfo->orgname] = $fieldInfo;
            }
        }

        return self::$_fieldInfoCache[$alias];
    }

    public function __toString()
    {
        return sprintf('[DB:mysql://%s/%s with user %s]', $this->host, $this->db, $this->user);
    }

    public function getDb()
    {
        return $this->db;
    }

    /**
     * @param callable $dumpMethod
     */
    public function setDumpMethod($dumpMethod)
    {
        if (!$dumpMethod) {
            $dumpMethod = function($dumpString) {
                var_dump($dumpString);
            };
        }
        $this->dumpMethod = $dumpMethod;
    }

    /**
     * @return callable
     */
    public function getDumpMethod()
    {
        return $this->dumpMethod;
    }
}

class DBExpression
{
    public static $NOW = 'NOW()';

    private $_expression;

    function __construct($expression)
    {
        $this->_expression = $expression;
    }

    function __toString()
    {
        return (string)$this->_expression;
    }
}


class DBUpdate
{
    private static $fieldInfoCache = array();
    private $_db;

    // private $pkId;
    // private $pkColumnName;
    public $pkColumns;

    protected $_table;
    protected $_joins = array();
    protected $fieldInfos;

    private $updates;

    /**
     * @param $dbConnection
     * @param $table
     * @param $pkId
     * @param string $pkColumnName
     * @return DBUpdate
     */
    public static function getSinglePKColumn(&$dbConnection, $table, $pkId, $pkColumnName = 'id')
    {
        $pkColumns = array($pkColumnName => $pkId);

        return new self($dbConnection, $table, $pkColumns);
    }

    // !!!!!!!! $pkColumnName must have a unique key to work properly !!!!!!!!!!
    /**
     * @param $dbConnection
     * @param $table
     * @param $pkColumns array($columnName, $columnValue) !!
     * @return DBUpdate
     */
    public static function getMultiPKColumn(&$dbConnection, $table, $pkColumns)
    {
        return new self($dbConnection, $table, $pkColumns);
    }

    public function __construct(DB &$dbConnection, $table, $pkColumns)
    {
        $this->_db = $dbConnection;
        $this->_table = $table;

        if (self::$fieldInfoCache[$table]) {
            $this->fieldInfos = self::$fieldInfoCache[$table];
        } else {
            // $this->db->debug(1);
            $result = $this->_db->query("SELECT * FROM $table LIMIT 1");
            while ($fieldInfo = $result->fetch_field()) {
                $this->fieldInfos[$fieldInfo->orgname] = $fieldInfo;
            }
            self::$fieldInfoCache[$table] = $this->fieldInfos;
        }

        $this->pkColumns = $pkColumns;
        if (!$this->pkColumns)
        {
            $this->pkColumns = array();

            foreach ($this->fieldInfos AS $fieldName => $fieldInfo)
            {
                if ($this->_db->checkIsPK($fieldInfo))
                {
                    $this->pkColumns[$fieldName] = $fieldInfo;
                }
            }
        }
    }

    /**
     * Sets a new value for a certain column EXCEPT for PK Columns. To Change PK column values, please use setNewPKValue()
     *
     * It magically detects the characteristics of the target column to forward data gracefully to database.
     * It takes care on charsets, escapings, enclosings, and a lot more.
     *
     * @param $columnName String ColumnName
     * @param $value      mixed If <code>NULL</code> and db field allows a NULL-value, it is saved as NULL, else as empty value
     * @return DBUpdate
     */
    public function setNewValue($columnName, $value)
    {
        if (!in_array($columnName, array_keys($this->pkColumns))) {
            $this->updates[$columnName] = $this->_db->mask($this->getFieldInfo($columnName), $value);
            return $this;
        } else {
            return false;
        }
    }

    /**
     * Little helper to mass update several columns at once settled in an array
     *
     * @param array $columnAndValues
     * @return $this
     */
    public function setNewValues(array $columnAndValues) {
        foreach ($columnAndValues AS $columnName => $value) {
            $this->setNewValue($columnName, $value);
        }
        return $this;
    }

    public function setNewValueToNow($columnName)
    {
        return $this->setNewValue($columnName, new DBExpression('NOW()'));
    }

    public function setNewPKValue($columnName, $value)
    {
        $this->updates[$columnName] = $this->_db->mask($this->getFieldInfo($columnName), $value);
        return $this;
    }

    public function join($alias, $joinToTable, $columnMapping, $joinFromTable = null, $joinType = 'INNER')
    {
        if (!$joinFromTable) {
            $joinFromTable = $this->_table;
        }

        $this->_joins[$alias] = array(
            'joinFromTable' => $joinFromTable,
            'joinToTable'   => $joinToTable,
            'columnMapping' => $columnMapping,
            'joinType'      => $joinType
        );

        // Cache fieldInfos of joinTable under it's alias name. needed ie. for contraints which always refers to aliases
        $this->_db->fieldInfo($joinToTable, $alias);

        return $this;
    }

    public function execute($mode = 'UPDATE', $isTestMode = false)
    {
        $sqlUpdates = array();
        $duplicateKeyUpdates = array();
        $suffix = '';

        foreach ((array)$this->updates AS $columnName => $value) {
            $sqlUpdates[] = "`{$this->_table}`.`$columnName` = $value";
            if (!$this->pkColumns[$columnName]) {
                $duplicateKeyUpdates[] = "`{$this->_table}`.`$columnName` = $value";
            }
        }
        $stmtStart = $mode;

        switch ($mode) {
            case 'INSERT INTO':
            case 'INSERT IGNORE INTO':
                $excludeColumns = array_merge((array)$this->updates, (array)$this->getPkColumnsAutoincrement());

                if ($pkColumns = $this->pkColumnsToSQL(', ', $excludeColumns)) {
                    array_unshift($sqlUpdates, $pkColumns);
                }
                break;
            case 'INSERT OR UPDATE':
                $stmtStart = 'INSERT INTO';
                $excludeColumns = array_merge((array)$this->updates, (array)$this->getPkColumnsAutoincrement());

                // Updates without PK columns!
                $suffix = "ON DUPLICATE KEY UPDATE " . implode(', ', $duplicateKeyUpdates);

                // For the insert into SET ... prepend the PK columns now
                if ($pkColumns = $this->pkColumnsToSQL(', ', $excludeColumns)) {
                    array_unshift($sqlUpdates, $pkColumns);
                }
                // array_unshift($sqlUpdates, $this->pkColumnsToSQL());
                break;
            case 'UPDATE':
                if (0 == count($this->updates)) {
                    return null; // no column need to be updated
                }

                if ($this->pkColumns) {
                    $suffix = " WHERE " . $this->pkColumnsToSQL(' AND ');
                }
                break;

            default:
                throw new \Exception('Invalid execution mode: ' . $mode);
        }

        $sqlJoinParts = array();
        foreach ($this->_joins AS $alias => $joinData) {
            $mappingParts = array();
            foreach ($joinData['columnMapping'] AS $fromColumn => $toColumn) {
                $mappingParts[] = sprintf(
                    '`%s`.`%s` = `%s`.`%s`',
                    $joinData['joinFromTable'],
                    $fromColumn,
                    $alias,
                    $toColumn
                );
            }
            $sqlJoinParts[] = sprintf(
                '%s%s JOIN `%s` AS %s ON (%s)',
                "\n",
                $joinData['joinType'],
                $joinData['joinToTable'],
                $alias,
                implode(' AND ', $mappingParts)
            );
        }

        $sql = sprintf(
            "{$stmtStart} {$this->_table}%s\nSET %s\n" . str_replace('%', '%%', $suffix),
            $sqlJoinParts ? implode(' ', $sqlJoinParts) : '',
            implode(', ', $sqlUpdates)
        );

        if ($isTestMode) {
            return $sql;
        } else {
            return $this->_db->query($sql);
        }
    }

    /**
     * @param bool $isTestMode
     * @param bool $ignoreDuplicateKeyErrors
     * @return int
     */
    public function insert($isTestMode = false, $ignoreDuplicateKeyErrors = false)
    {
        $insertSQL = $ignoreDuplicateKeyErrors ? 'INSERT IGNORE INTO' : 'INSERT INTO';

        if ($res = $this->execute($insertSQL, $isTestMode)) {
            return ($isTestMode) ? $res : $this->_db->con->insert_id;
        } else {
            return false;
        }
    }

    public function insertOrUpdate($isTestMode = false)
    {
        if ($res = $this->execute('INSERT OR UPDATE', $isTestMode)) {
            return ($isTestMode) ? $res : $this->_db->con->insert_id;
        } else {
            return false;
        }
    }

    public function update($isTestMode = false)
    {
        return $this->execute('UPDATE', $isTestMode);
    }

    /**
     * @param null $columnName
     * @return mixed
     */
    public function getFieldInfo($columnName = null)
    {
        return $columnName ? $this->fieldInfos[$columnName] : $this->fieldInfos;
    }

    public function getNewValues()
    {
        return $this->updates;
    }

    public function pkColumnsToSQL($separator = ', ', &$excludeColumns = null)
    {
        $columnParts = null;

        $columns = $this->pkColumns;
        if (is_array($excludeColumns)) {
            $columns = array_diff_key($columns, $excludeColumns);
        }

        foreach ($columns AS $column => $value) {
            if ($columnInfo = $this->getFieldInfo($column)) {
                $columnParts[] = "`{$this->_table}`.`{$column}` = " . $this->_db->mask($columnInfo, $value);
            } else {
                $columnParts[] = "`{$column}` = '{$value}'";
            }
        }

        return is_array($columnParts) ? implode($separator, $columnParts) : null;
    }

    protected function getPkColumnsAutoincrement() {
        $autoincrementColumns = array();

        foreach ($this->pkColumns AS $fieldName => $fieldInfo)
        {
            if ($this->_db->checkIsAutoIncrement($fieldInfo)) {
                $autoincrementColumns[$fieldName] = $fieldInfo;
            }
        }

        return $autoincrementColumns;
    }
}


class DBSelect
{
    public $_table;
    private $_fieldInfos = array();
    private $_joins = array();
    private $_constraints = array();
    private $_sort;
    private $_group = array(); /* $group[]['table'], $group[]['column'] */
    private $_columnsToSelect = array(); /* $c2s[]['table'], c2s[]['column'], c2s[]['as'] */
    private $_unions = array();

    const EXPLAIN_NO = 0;
    const EXPLAIN = 1;
    const EXPLAIN_EXTENDED = 2;
    private $_explain = self::EXPLAIN_NO;

    private $_measure = false;

    /**
     * @var
     */
    private $_limitTo;
    private $_limitFrom = 0;

    public function __construct(DB $db, $table)
    {
        $this->_db = $db;
        $this->_table = $table;

        $this->_fieldInfos = $this->_db->fieldInfo($table);
    }

    public function joinLeft($alias, $joinToTable, $columnMapping, $joinFromTable = null, $customJoinPart = null)
    {
        return $this->join($alias, $joinToTable, $columnMapping, $joinFromTable, 'LEFT', $customJoinPart);
    }

    /**
     * This adds a join to your select
     *
     * @param $alias
     * @param $joinToTable
     * @param $columnMapping Array This funny contruct maps 2 tables together. The key is the source column of the table
     *          to join from, the value is the target column of the table to are joining to. If you need to set a column
     *          of your target table to a certain value, you can pass an integer as an key, which will result to
     *          `targettable`.`targetcolumn` = integer
     * @param null $joinFromTable
     * @param string $joinType
     * @return $this
     */
    public function join(
        $alias,
        $joinToTable,
        $columnMapping,
        $joinFromTable = null,
        $joinType = 'INNER',
        $customJoinPart = null
    ) {
        if (!$joinFromTable) {
            $joinFromTable = $this->_table;
        }

        $this->_joins[$alias] = array(
            'joinFromTable'  => $joinFromTable,
            'joinToTable'    => $joinToTable,
            'columnMapping'  => $columnMapping,
            'joinType'       => $joinType,
            'customJoinPart' => $customJoinPart
        );

        // Cache fieldInfos of joinTable under it's alias name. needed ie. for contraints which always refers to aliases
        $this->_db->fieldInfo($joinToTable, $alias);

        return $this;
    }

    public function constraintBetween($column, $betweenFrom, $betweenTo)
    {
        return $this->constraintBetweenTable($this->_table, $column, $betweenFrom, $betweenTo);
    }

    public function constraintBetweenTable($table, $column, $betweenFrom, $betweenTo)
    {
        $this->constraintCompareTable($table, $column, array($betweenFrom, $betweenTo), 'BETWEEN');

        return $this;
    }

    public function constraintNotBetween($column, $betweenFrom, $betweenTo)
    {
        return $this->constraintNotBetweenTable($this->_table, $column, $betweenFrom, $betweenTo);
    }

    public function constraintNotBetweenTable($table, $column, $betweenFrom, $betweenTo)
    {
        $this->constraintCompareTable($table, $column, array($betweenFrom, $betweenTo), 'NOT BETWEEN');

        return $this;
    }

    public function constraintCompare($column, $condition, $operator)
    {
        return $this->constraintCompareTable($this->_table, $column, $condition, $operator);
    }

    public function constraintCompareTable($table, $column, $values, $comparator)
    {
        $this->_constraints[$column][] =
            array(
                'table'    => $table,
                'column'   => $column,
                'operator' => $comparator,
                'value'    => $values
            );

        return $this;
    }

    public function constraintIsBlankOrNull($column)
    {
        return $this->constraintIsBlankOrNullTable($this->_table, $column);
    }

    public function constraintIsBlankOrNullTable($table, $column)
    {
        return $this->constraintCompareTable($table, $column, null, 'EMPTY');
    }

    public function constraintIsNull($column)
    {
        return $this->constraintIsNullTable($this->_table, $column);
    }

    public function constraintIsNullTable($table, $column)
    {
        return $this->constraintCompareTable($table, $column, new DBExpression('IS NULL'), null);
    }

    public function constraintIsNotNull($column)
    {
        return $this->constraintIsNotNullTable($this->_table, $column);
    }

    public function constraintIsNotNullTable($table, $column)
    {
        return $this->constraintCompareTable($table, $column, new DBExpression('IS NOT NULL'), null);
    }

    public function constraintIn($column, $values)
    {
        return $this->constraintInTable($this->_table, $column, $values);
    }

    public function constraintInTable($table, $column, $values)
    {
        return $this->constraintCompareTable($table, $column, $values, 'IN');
    }

    public function constraintEquals($column, $condition)
    {
        return $this->constraintEqualsTable($this->_table, $column, $condition);
    }

    public function constraintStartsWith($column, $condition)
    {
        return $this->constraintStartsWithTable($this->_table, $column, $condition);
    }

    public function constraintStartsWithTable($table, $column, $condition)
    {
        return $this->constraintLikeTable($table, $column, $condition . '%');
    }

    public function constraintNotStartsWith($column, $condition)
    {
        return $this->constraintNotStartsWithTable($this->_table, $column, $condition);
    }

    public function constraintNotStartsWithTable($table, $column, $condition)
    {
        return $this->constraintNotLikeTable($table, $column, $condition . '%');
    }

    public function constraintLike($column, $condition)
    {
        return $this->constraintLikeTable($this->_table, $column, $condition);
    }

    public function constraintLikeTable($table, $column, $condition)
    {
        if (!DB::checkForFieldset($this->_db->fieldInfoColumn($table, $column), COLUMN_FIELD_SET_STRING)) {
            throw new Exception('Like will work only on text columns');
        }
        return $this->constraintCompareTable($table, $column, $condition, 'LIKE');
    }

    public function constraintNotLike($column, $condition)
    {
        return $this->constraintNotLikeTable($this->_table, $column, $condition);
    }

    public function constraintNotLikeTable($table, $column, $condition)
    {
        if (!DB::checkForFieldset($this->_db->fieldInfoColumn($table, $column), COLUMN_FIELD_SET_STRING)) {
            throw new Exception('Like will work only on text columns');
        }
        return $this->constraintCompareTable($table, $column, $condition, 'NOT LIKE');
    }

    public function constraintEqualsTable($table, $column, $value)
    {
        return $this->constraintCompareTable($table, $column, $value, '=');
    }

    public function constraintNotEquals($column, $condition)
    {
        return $this->constraintNotEqualsTable($this->_table, $column, $condition);
    }

    public function constraintNotEqualsTable($table, $column, $condition)
    {
        return $this->constraintCompareTable($table, $column, $condition, '!=');
    }

    public function constraintCustom($customSQL)
    {
        $this->_constraints['DB#custom'][] = $customSQL;
        return $this;
    }

    public function sort($columns)
    {
        if (is_array($columns)) {
            $columns = implode(', ', $columns);
        }

        $this->_sort = $columns;

        return $this;
    }

    public function addGroup($column, $table = null)
    {
        $this->_group[] = array(
            'table'  => $table ? $table : $this->_table,
            'column' => $column
        );

        return $this;
    }

    public function group($columns)
    {
        if (!is_array($columns)) {
            $columns = array($columns);
        }

        foreach ($columns AS $columnString) {
            $table = $this->_table;
            $column = $columnString;

            if (strpos($columnString, '.') !== false) {
                list ($table, $column) = explode('.', $columnString);
            }

            $groupArray = array();
            $groupArray['table'] = $table;
            $groupArray['column'] = $column;

            $this->_group[] = $groupArray;
        }

        return $this;
    }

    public function customSort($customSort)
    {
        $this->_sort = $customSort;

        return $this;
    }

    /**
     * @param $limitTo
     * @param int $limitFrom
     * @return DBSelect
     */
    public function limit($limitTo, $limitFrom = 0)
    {
        $this->_limitTo = $limitTo;
        $this->_limitFrom = $limitFrom;

        return $this;
    }

    public function addUnion(DBSelect $select)
    {
        $this->_unions[] = $select;
    }

    public function explain($explainMode = self::EXPLAIN)
    {
        $this->_explain = $explainMode;

        return $this->fetchAll();
    }

    public function measure()
    {
        $this->_measure = true;

        return $this;
    }

    public function sql()
    {
        $sqlJoinParts = array();
        foreach ($this->_joins AS $alias => $joinData) {
            $mappingParts = array();
            foreach ($joinData['columnMapping'] AS $fromColumn => $toColumn) {

                if (is_numeric($fromColumn)) {
                    $mappingParts[] = sprintf(
                        '`%s`.`%s` = %s',
                        $alias,
                        $toColumn,
                        $fromColumn
                    );
                } else {
                    $mappingParts[] = sprintf(
                        '`%s`.`%s` = `%s`.`%s`',
                        $joinData['joinFromTable'],
                        $fromColumn,
                        $alias,
                        $toColumn
                    );
                }
            }
            if ($joinData['customJoinPart']) {
                $mappingParts[] = $joinData['customJoinPart'];
            }

            $sqlJoinParts[] = sprintf(
                '%s%s JOIN `%s` AS %s ON (%s)',
                "\n",
                $joinData['joinType'],
                $joinData['joinToTable'],
                $alias,
                implode(' AND ', $mappingParts)
            );
        }

        $conditionParts = array();
        foreach ($this->_constraints AS $columnName => $constraints) {
            foreach ($constraints AS $constraint) {
                if ('DB#custom' == $columnName) {
                    $conditionParts[] = $constraint;
                } else {
                    $fieldInfo = $this->_db->fieldInfo($constraint['table']);

                    $values = $constraint['value'];
                    if (!is_array($values)) {
                        $values = array($values);
                    }

                    // Mask all values if neccessary
                    for ($i = 0; $i < count($values); $i++) {
                        if (!($values[$i] instanceof DBExpression)) {
                            $values[$i] = $this->_db->mask($fieldInfo[$columnName], $values[$i]);
                        }
                    }

                    if (in_array($constraint['operator'], array('BETWEEN', 'NOT BETWEEN'))) {
                        $value = $values[0] . ' AND ' . $values[1];
                    } else if ($constraint['operator'] == 'EMPTY') {
                        $constraint['operator'] = 'IS';
                        $value = ' NULL OR `' . $constraint['table'] . '`.`' . $columnName . "` = ''";
                    } else if (count($values) > 1) {
                        $value = '(' . implode(', ', $values) . ')';
                    } else {
                        $value = $values[0];
                        if ($constraint['operator'] == 'IN') {
                            $constraint['operator'] = '=';
                        }
//                        if (is_null($constraint['value'])) {
//                            $constraint['operator'] = null;
//                            $value = 'IS NULL';
//                        }
                    }

                    $conditionParts[] = sprintf(
                        '`%s`.`%s` %s %s',
                        $constraint['table'],
                        $columnName,
                        $constraint['operator'],
                        $value
                    );
                }
            }
        }
        $sqlConditions = implode("\nAND ", $conditionParts);

        if (!$this->_columnsToSelect) {
            $this->_columnsToSelect[] = array(
                'table'  => null,
                'column' => new DBExpression('*'),
                'as'     => null
            );
        }

        $columnSQLParts = array();
        foreach ($this->_columnsToSelect AS $column) {
            $columnSQLPart = '';

            if ($column['column'] instanceof DBExpression) {
                $columnSQLPart .= $column['column'];
            } else {
                $columnSQLPart .= "`{$column['table']}`.`{$column['column']}`";
            }

            if ($as = $column['as']) {
                $columnSQLPart .= " AS `$as`";
            }

            $columnSQLParts[] = $columnSQLPart;
        }

        $columnsToSelect = implode(', ', $columnSQLParts);

        $groupConditions = '';
        if ($this->_group) {
            $groupConditionsArray = array();

            foreach ($this->_group AS $groupCondition) {
                if ($groupCondition['column'] instanceof DBExpression) {
                    $groupConditionsArray[] = $groupCondition['column'];
                } else {
                    $groupConditionsArray[] = sprintf(
                        '`%s`.`%s`',
                        $groupCondition['table'],
                        $groupCondition['column']
                    );
                }
            }

            $groupConditions = implode(', ', $groupConditionsArray);
        }

        $explain = '';
        switch ($this->_explain) {
            case self::EXPLAIN:
                $explain = 'EXPLAIN ';
                break;
            case self::EXPLAIN_EXTENDED:
                $explain = 'EXPLAIN EXTENDED';
                break;
        }

        $sql = sprintf(
            "%sSELECT %s%s\nFROM %s%s%s%s",
            $explain,
            $this->_measure ? 'SQL_NO_CACHE ' : '',
            $columnsToSelect,
            $this->_table,
            $sqlJoinParts ? ' ' . implode(' ', $sqlJoinParts) : '',
            $sqlConditions ? "\nWHERE " . $sqlConditions : '',
            $groupConditions ? "\nGROUP BY " . $groupConditions : ''
        );

        if ($this->_unions) {
            $unionSQLs = array_map(
                function (DBSelect $union) {
                    return $union->sql();
                },
                $this->_unions
            );

            array_unshift($unionSQLs, $sql);

            $sql = implode("\nUNION\n", $unionSQLs);
        }

        if ($this->_sort) {
            $sql .= "\nORDER BY " . $this->_sort;
        }

        if ($this->_limitTo) {
            if ($this->_limitFrom) {
                $sqlLimit = sprintf("\nLIMIT %s, %s", $this->_limitFrom, $this->_limitTo);
            } else {
                $sqlLimit = sprintf("\nLIMIT %s", $this->_limitTo);
            }
            $sql .= $sqlLimit;
        }

        return $sql;
    }

    /**
     * @deprecated Please use fieldInfo() of class DB (this one has multi-table support)
     * @param null $columnName
     * @return array
     */
    public function getFieldInfo($columnName = null)
    {
        return $columnName ? $this->_fieldInfos[$columnName] : $this->_fieldInfos;
    }

    /**
     * Selects a certain column via default-table.$column
     *
     * @param $columns String|Array
     * @param $as
     * @return DBSelect
     */
    public function column($columns, $as = null)
    {
        if (!is_array($columns)) {
            $as = $as ? $as : $columns;
            $columns = array($as => $columns);
        }


        foreach ($columns AS $as => $column) {
            $this->_columnsToSelect[] = array(
                'table'  => $this->_table,
                'column' => $column,
                'as'     => ($column != $as && !is_numeric($as)) ? $as : null
            );
        }

        return $this;
    }

    /**
     * Selects a certain column via $table.$column
     *
     * @param $column String
     * @param $table
     * @param $as
     * @return DBSelect
     */
    public function columnOfTable($column, $table = null, $as = null)
    {
        $this->_columnsToSelect[] = array(
            'table'  => $table ? $table : $this->_table,
            'column' => $column,
            'as'     => $as
        );

        return $this;
    }

    /**
     * Selects a certain column via custom expression
     *
     * @param $columnExpression String Custom Expression
     * @param $as
     * @return DBSelect
     */
    public function columnByExpression($columnExpression, $as = null)
    {
        $this->_columnsToSelect[] = array(
            'table'  => null,
            'column' => new DBExpression($columnExpression),
            'as'     => $as
        );

        return $this;
    }

    public function getColumnExpression($index)
    {
        return $this->_columnsToSelect[$index];
    }

    /**
     * Selects a set of columns via a custom DBExpression
     *
     * @deprecated Do not use custom Expressions if you do not need to!
     * @param null $columnsToSelect
     * @return DBSelect
     */
    public function columnsToSelect($columnsToSelect)
    {
        $this->_columnsToSelect = array();

        if (!is_array($columnsToSelect)) {
            $columnsToSelect = array($columnsToSelect);
        }

        foreach ($columnsToSelect AS $customExpression) {
            $this->columnByExpression($customExpression);
        }

        return $this;
    }

    public function fetchAll($columnToRow = null)
    {
        $start = microtime(true);
        $return = $this->_db->queryAndFetchAllSelect($this, $columnToRow);

        if ($this->_measure) {
            return microtime(true) - $start;
        }
        return $return;
    }

    public function fetchColumn($column = null)
    {
        $start = microtime(true);
        $return = $this->_db->queryAndFetchSingleColumnSelect($this, $column);

        if ($this->_measure) {
            return microtime(true) - $start;
        }
        return $return;
    }

    public function fetchRow()
    {
        $start = microtime(true);
        $return = $this->_db->queryAndFetchArraySelect($this);

        if ($this->_measure) {
            return microtime(true) - $start;
        }
        return $return;
    }

    public function fetchCell($columnName = null)
    {
        if ($columnName) {
            // It's enough to select only this single column
            $this->column($columnName);
        }

        $start = microtime(true);
        $return = $this->_db->querySingleValueSelect($this, $columnName);

        if ($this->_measure) {
            return microtime(true) - $start;
        }
        return $return;
    }

    public function count()
    {
        $start = microtime(true);
        $return = $this->_db->countSelect($this);

        if ($this->_measure) {
            return microtime(true) - $start;
        }
        return $return;
    }

    /**
     * Wraps the DB->delete() method to be callable directly in an DBSelect. Use $db->affectedRows() to count the
     * affected rows of your deletion.
     *
     * @param null|string|array $tableAliases The table aliases to delete once rows (default is the main table to join from)
     * @param bool $isTestMode
     * @return bool|mixed|mysqli_result
     */
    public function deleteAllRows($tableAliases = null, $isTestMode = false)
    {
        return $this->_db->delete($this, $tableAliases, $isTestMode);
    }
}

class DBTableColumn
{
    private $_fieldName;
    private $_fieldType; // Any value of static::$fieldTypes
    private $_fieldLength;
    private $_fieldCanBeNull;
    private $_fieldIsPK;
    private $_fieldIsAutoincrement;


    public static $fieldTypes = array(
        'FLOAT'    => MYSQLI_TYPE_FLOAT,
        'INT'      => MYSQLI_TYPE_LONG,
        'TINYINT'  => MYSQLI_TYPE_TINY,
        'VARCHAR'  => MYSQLI_TYPE_VAR_STRING,
        'BLOB'     => MYSQLI_TYPE_BLOB,
        'TEXT'     => MYSQLI_TYPE_BLOB,
        'CHAR'     => MYSQLI_TYPE_CHAR,
        'DATETIME' => MYSQLI_TYPE_DATETIME,
        'ENUM' => MYSQLI_TYPE_ENUM,
    );

    public static function instanceFromFieldInfo(stdClass $fieldInfo)
    {
        return new DBTableColumn(
            $fieldInfo->orgname,
            $fieldInfo->type,
            $fieldInfo->length,
            DB::checkCanBeNull($fieldInfo),
            DB::checkIsPK($fieldInfo),
            DB::checkIsAutoIncrement($fieldInfo)
        );
    }

    function __construct(
        $fieldName,
        $fieldType,
        $fieldLength,
        $fieldCanBeNull = false,
        $fieldIsPK = false,
        $fieldIsAutoincrement = false
    ) {
        $this->_fieldName = $fieldName;
        $this->_fieldType = $fieldType;
        $this->_fieldLength = $fieldLength;
        $this->_fieldCanBeNull = $fieldCanBeNull;
        $this->_fieldIsPK = $fieldIsPK;
        $this->_fieldIsAutoincrement = $fieldIsAutoincrement;
    }

    /**
     * @return boolean
     */
    public function fieldIsPK()
    {
        return $this->_fieldIsPK;
    }

    public function setFieldIsPK($fieldIsPK = true)
    {
        $this->_fieldIsPK = $fieldIsPK;
    }

    /**
     * @return mixed
     */
    public function fieldName()
    {
        return $this->_fieldName;
    }

    /**
     * @return mixed
     */
    public function fieldType()
    {
        return $this->_fieldType;
    }

    /**
     * @return mixed
     */
    public function fieldLength()
    {
        return $this->_fieldLength;
    }

    /**
     * @return boolean
     */
    public function fieldCanBeNull()
    {
        return $this->_fieldCanBeNull;
    }

    /**
     * @return boolean
     */
    public function fieldIsAutoincrement()
    {
        return $this->_fieldIsAutoincrement;
    }

}

class DBTable
{
    private static $_fieldInfoCache = array();

    private $_db;
    private $_table;
    private $_existingFieldInfos = array();
    private $_updateFieldInfos = array();

    public function __construct(DB $db, $table)
    {
        $this->_db = $db;
        $this->_table = $table;
    }

    public function addColumn(
        $fieldName,
        $fieldType,
        $fieldLength = null,
        $fieldCanBeNull = false,
        $fieldIsPK = false,
        $fieldIsAutoincrement = false
    ) {
        $this->_updateFieldInfos[$fieldName] = new DBTableColumn(
            $fieldName,
            $fieldType,
            $fieldLength,
            $fieldCanBeNull,
            $fieldIsPK,
            $fieldIsAutoincrement
        );
    }

    public function alterTable()
    {
//      foreach (array_merge($this->_existingFieldInfos, $this->_updateFieldInfos) AS $field) {
        throw new Exception('Alter existing table is not implemented yet!');

    }

    public function createTable($ifNotExists = false, $charset = 'utf8', $engine = 'MyISAM')
    {
        $fieldTypeCode2fieldTypeName = array_flip(DBTableColumn::$fieldTypes);

        $alterColumnSQL = array();

        foreach ($this->_updateFieldInfos AS $field) {
            if ($field instanceof DBTableColumn) {
                $mysqlFieldType = $fieldTypeCode2fieldTypeName[$field->fieldType()];

                //   `id` int(11) NOT NULL,
                $alterColumnSQL[] = sprintf(
                    '`%s` %s%s%s%s',
                    $field->fieldName(),
                    $mysqlFieldType ? $mysqlFieldType : 'TEXT',
                    $field->fieldLength() ? '(' . $field->fieldLength() . ')' : '',
                    !$field->fieldCanBeNull() ? ' NOT NULL' : '',
                    $field->fieldIsAutoincrement() ? ' AUTO_INCREMENT' : ''
                );
            } else {
                throw new \Exception('Something strange is going on');
            }
        }

        if ($pkColumns = $this->getPKColumns()) {

            foreach ($pkColumns AS $pkColumn) {
                $pkColumnSQLPart[] = $pkColumn->fieldName();
            }
            $alterColumnSQL[] = sprintf('PRIMARY KEY (%s)', '`' . implode('`, `', $pkColumnSQLPart) . '`');
        }

        $ifNotExistsSQL = '';
        if ($ifNotExists) {
            $ifNotExistsSQL = ' IF NOT EXISTS';
        }

        $sql = sprintf(
            "CREATE TABLE{$ifNotExistsSQL} `%s`.`%s` (%s) ENGINE=%s DEFAULT CHARSET=%s",
            $this->_db->getDb(),
            $this->_table,
            implode(', ', $alterColumnSQL),
            $engine,
            $charset
        );

        return $this->_db->query($sql);
    }

    public function existingFieldInfos()
    {
        if (self::$_fieldInfoCache[$this->_table]) {
            $this->_existingFieldInfos = self::$_fieldInfoCache[$this->_table];
        } else {
            if ($this->tableExists()) {
                // $this->_db->debug(1);
                $result = $this->_db->query("SELECT * FROM {$this->_table} LIMIT 1");
                while ($fieldInfo = $result->fetch_field()) {
                    $this->_existingFieldInfos[$fieldInfo->orgname] = DBTableColumn::instanceFromFieldInfo($fieldInfo);
                }
                self::$_fieldInfoCache[$this->_table] = $this->_existingFieldInfos;
            } else {
                self::$_fieldInfoCache[$this->_table] = array();

            }
        }

        return self::$_fieldInfoCache[$this->_table];
    }

    public function tableExists()
    {
        return $this->_table == $this->_db->querySingleValue("SHOW TABLES LIKE '{$this->_table}'");
    }

    /**
     * Deletes all rows of the table
     *
     * @return bool|\mysqli_result
     */
    public function truncate()
    {
        return $this->_db->query("TRUNCATE TABLE `{$this->_table}`");
    }

    public function removeTable()
    {
        $this->_db->query("DROP TABLE IF EXISTS `{$this->_table}`");
    }

    private function getPKColumns()
    {
        $pkColumns = null;

        foreach (array_merge($this->_existingFieldInfos, $this->_updateFieldInfos) AS $field) {
            if ($field instanceof DBTableColumn && $field->fieldIsPK()) {
                $pkColumns[] = $field;
            }
        }

        return $pkColumns;
    }
}