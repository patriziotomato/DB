<?php
namespace ScoopDb;

interface DBUpdateStatements {
    /**
     * Returns a list of all defined Database Updates in an array.
     *
     * @return array Key = unique update number, value = (see examples)
     * Examples:
     *
     * $updates[10]["description"] = "Test Insert";
     * $updates[10]["operations_sql"][] = "INSERT INTO dbupdates SET `dbupdates`.`dbupdate_id` = 999, `dbupdates`.`dbupdate_execution_status` = 'skipped', `dbupdates`.`dbupdate_execution_date` = '', `dbupdates`.`dbupdate_execution_duration` = 0, `dbupdates`.`dbupdate_description` = '', `dbupdates`.`dbupdate_execution_feedback` = ''";
     *
     * $updates[20]["description"] = "PHP Closure Test";
     * $updates[20]["operation_php"] = function (DB $db)
     * {
     *     sleep(1);
     *     return "PHP Code executed";
     * };
     */
    function getUpdateStatements();
}

class DBUpdatesSuspendJobException extends \Exception
{
    // Führt dazu, dass ein angehaltener Job die gesamte Ausführung stoppt (Erst muss dieser Job komplett
    // durchlaufen, bevor er mit anderen Jobs weitermachen kann). Wir bei kritischen DB Änderungen z.B. notwendig
    // sein
    const MODE_SUSPEND_OTHERS = 1;

    // Führt dazu, dass ein angehaltener Job, sofern die Zeit für diesen Durchlauf noch nicht abgelaufen ist,
    // weitermacht. Dies ist für Jobs ohne Abhängigkeiten sehr gut geeignet und blockiert keine anderen Jobs.
    // Damit er mit anderen Job jeweils weitermacht, ist es allerdings notwendig, dass die Laufzeit nicht/nie die
    // gesamte Laufzeit die zur Verfügung steht übersteigt, sonst macht er im nächsten Durchlauf ja wieder mit
    // diesem Job weiter
    const MODE_SUSPEND_CONTINUE = 2;


    private $str_mode = self::MODE_SUSPEND_OTHERS;

    // Diese Exception kann in einem "Job" geworfen werden, wenn die aktuelle ausführung unterbrochen werden soll
    // Damit lässt sich ein Job auf mehrere Durchläufe aufteilen

    public function __construct($message = "", $str_mode = self::MODE_SUSPEND_OTHERS, $code = 0, \Exception $previous = null)
    {
        $this->str_mode = $str_mode;

        parent::__construct($message, $code, $previous);
    }

    /**
     * @param int $int_mode See constants MODE_*
     * @return bool
     */
    public function is_mode($int_mode)
    {
        return $this->str_mode == $int_mode;
    }
}

class DBUpdates {
    public static $tableDbUpdates = 'dbupdates';

    const BREAK_AFTER_SECONDS_DEFAULT = 300;

    /**
     * @var DB
     */
    private $_db;
    private $_breakAfterSeconds;
    private $_updates = array();

    function __construct(DB $db, DBUpdateStatements $updates, $breakAfterSeconds = self::BREAK_AFTER_SECONDS_DEFAULT)
    {
        $this->_db = $db;
        $this->_breakAfterSeconds = $breakAfterSeconds;

        if ($updates) {
            $this->_updates = $updates->getUpdateStatements();
        }

        $this->createDBLogTableIfNotExists();
    }


    public function execute() {

        $time_total_start = microtime(true);

        // Welche Updates wurden bereits ausgeführt?
        $updatesAusgefuehrt = $this->_db->select(static::$tableDbUpdates)->fetchAll('dbupdate_id');

        // Welche müssen daher noch ausgeführt werden?
        $updatesNochNichtAusgefuehrt = $this->ermittle_updates($updatesAusgefuehrt);

        $int_anzahl_abgearbeitete_updates = 0;
        $int_anzahl_angehaltene_updates = 0;

        // Jetzt mit den auszuführenden Updates weitermachen...
        foreach ($updatesNochNichtAusgefuehrt AS $int_update_nummer => $array_update_daten)
        {
            $bit_break_after_run = false;

            $array_daten = array();
            $array_daten["dbupdate_id"] = $int_update_nummer;
            $array_daten["dbupdate_execution_date"] = new DBExpression('NOW()');
            $array_daten["dbupdate_description"] = $array_update_daten["description"];
            $array_daten["dbupdate_execution_status"] = "executed";

            $time_start = microtime(true);
            $str_results = "";

            // Sollen einfache SQL Statements ausgeführt werden?
            if ($array_update_daten["operations_sql"])
            {
                $i = 0;
                $str_result = array();

                foreach ($array_update_daten["operations_sql"] AS $str_sql)
                {

                    $str_result[$i] = "Executed SQL";
                    $str_result[$i].= "in " . number_format((microtime(true) - $time_start), 3) . "s";
                    $str_result[$i].= " with result: ";

                    /**
                     * @var $result bool (Or no data manipulation statement!?)
                     */
                    $result = $this->_db->query($str_sql);

                    $str_result[$i].= is_object($result) ? 'no data manipulation statement' : $result;
                    $str_result[$i].= ". Statement: " . $str_sql;

                    $i++;
                }

                $str_results = implode("\n", $str_result);
            }

            // Soll ein PHP Block ausgeführt werden?
            if ($array_update_daten["operation_php"])
            {
                $closure_php_block = $array_update_daten["operation_php"];

                try
                {
                    $str_return = $closure_php_block($this->_db);
                }
                catch (DBUpdatesSuspendJobException $e)
                {
                    $int_anzahl_angehaltene_updates++;
                    $array_daten["dbupdate_execution_status"] = "running";

                    $str_return = $e->getMessage();

                    if ($e->is_mode(DBUpdatesSuspendJobException::MODE_SUSPEND_OTHERS))
                    {
                        $bit_break_after_run = true;
                    }
                }

                $str_results = "Executed PHP Block";
                $str_results .= " with result: " . $str_return;
            }

            // Wenn der Job schon mal gelaufen ist und schon Feedback vorhanden war, wird das alte Feedback angehängt
            if ($str_old_feedback = $array_update_daten["last_run"]["dbupdate_execution_feedback"])
            {
                $str_results .= "\n".$str_old_feedback;
            }

            $array_daten["dbupdate_execution_duration"] = microtime(true) - $time_start;
            $array_daten["dbupdate_execution_feedback"] = $str_results;

            // Der Job lief schon einmal?
            if ($array_last_run = $array_update_daten["last_run"])
            {
                $update = $this->_db->prepareUpdate(static::$tableDbUpdates, $int_update_nummer, 'dbupdate_id');
                $update->setNewValues($array_daten);
                $update->update();
            }
            else
            {
                $insert = $this->_db->prepareInsert(static::$tableDbUpdates);
                $insert->setNewPKValue('dbupdate_id', $array_daten['dbupdate_id']); // dbupdate_id ist kein autoincrement!
                $insert->setNewValues($array_daten);
                $insert->insert();
            }

            $int_anzahl_abgearbeitete_updates++;

            $time_total_duration = microtime(true) - $time_total_start;

            if ($bit_break_after_run || $time_total_duration > $this->_breakAfterSeconds)
            {
                if ($bit_break_after_run)
                {
//                    $obj_logger->log(jetzt()." Suspended job after a total execution time of ".number_format($time_total_duration, 3)."s", "info");
                }
                else
                {
//                    $obj_logger->log(jetzt()." Paused job after a total execution time of ".number_format($time_total_duration, 3)."s", "info");
                }
                break;
            }
        }

        return $int_anzahl_abgearbeitete_updates;
    }

    private function ermittle_updates(array $updatesAusgefuehrt) {
        $updatesNochNichtAusgefuehrt = array();
        foreach ($this->_updates AS $idx => $updateData)
        {
            // Wurde die Operation evtl. schon ausgeführt?
            if ($array_existing_update = $updatesAusgefuehrt[$idx])
            {
                // Die Operation lief schon mal, aber war noch nicht fertig?
                if ($array_existing_update["dbupdate_execution_status"] == "running")
                {
                    $updateData["last_run"] = $array_existing_update;
                }
                else
                {
                    // Die Operation lief schon mal und ist entweder fertig oder geskipped worden
                    continue;
                }
            }

            // Die Operation wurde noch nicht ausgeführt
            $updatesNochNichtAusgefuehrt[$idx] = $updateData;
        }

        // Gebe nur die Array-Elemente zurück, die nicht bereits (vollständig) ausgeführt wurden
        return $updatesNochNichtAusgefuehrt;
    }

    private function createDBLogTableIfNotExists() {

        $dbTable = new DBTable($this->_db, static::$tableDbUpdates);

        if (!$dbTable->tableExists()) {
            $dbTable->addColumn(
                $fieldName = 'dbupdate_id',
                $fieldType = DBTableColumn::$fieldTypes['INT'],
                $fieldLength = 11,
                $fieldCanBeNull = false,
                $fieldIsPK = true,
                $fieldIsAutoincrement = false
            );

            $dbTable->addColumn(
                $fieldName = 'dbupdate_execution_status',
                $fieldType = DBTableColumn::$fieldTypes['ENUM'],
                $fieldLength = "'skipped','executed','running'"
            );

            $dbTable->addColumn(
                $fieldName = 'dbupdate_execution_date',
                $fieldType = DBTableColumn::$fieldTypes['DATETIME'],
                $fieldLength = null
            );

            $dbTable->addColumn(
                $fieldName = 'dbupdate_execution_duration',
                $fieldType = DBTableColumn::$fieldTypes['FLOAT'],
                $fieldLength = null
            );

            $dbTable->addColumn(
                $fieldName = 'dbupdate_description',
                $fieldType = DBTableColumn::$fieldTypes['TEXT'],
                $fieldLength = null
            );

            $dbTable->addColumn(
                $fieldName = 'dbupdate_execution_feedback',
                $fieldType = DBTableColumn::$fieldTypes['TEXT'],
                $fieldLength = null
            );

            $dbTable->createTable($ifNotExists = true);
        }
    }
}