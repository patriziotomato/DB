<?php

namespace tests\classes;

use ScoopDb\DB;
use ScoopDb\DBTable;
use ScoopDb\DBUpdates;
use ScoopDb\DBUpdatesSuspendJobException;
use ScoopDb\DBUpdateStatements;

error_reporting(E_ALL ^ E_NOTICE);

define('DB_HOST', "db2.scoop-medianet.de");
define('DB_DATABASE', "scoop_db");
define('DB_USER', "scoop_db");
define('DB_PWD', "aZTXZ2WT");

require_once __DIR__.'/../../vendor/autoload.php';
require_once __DIR__.'/../../src/ScoopDb/DB.php';
require_once __DIR__.'/../../src/ScoopDb/DBUpdates.php';

class DBUpdatesMock implements DBUpdateStatements {
    public $updates;

    function __construct()
    {
        $updates = array();

        $updates[10]["description"] = "Test Insert";
        $updates[10]["operations_sql"][] = "INSERT INTO dbupdates SET `dbupdates`.`dbupdate_id` = 999, `dbupdates`.`dbupdate_execution_status` = 'skipped', `dbupdates`.`dbupdate_execution_date` = '', `dbupdates`.`dbupdate_execution_duration` = 0, `dbupdates`.`dbupdate_description` = '', `dbupdates`.`dbupdate_execution_feedback` = ''";

        $updates[20]["description"] = "PHP Closure Test";
        $updates[20]["operation_php"] = function (DB $db)
        {
            sleep(1);
            return "PHP Code executed";
        };

        $this->updates = $updates;
    }

    function getUpdateStatements()
    {
        return $this->updates;
    }

}


class DBUpdatesTest extends \PHPUnit_Framework_TestCase
{
    private function removeDbUpdatesTable() {
        $db = DB::get();
        $table = new DBTable($db, DBUpdates::$tableDbUpdates);

        $table->removeTable();

        return $table;
    }

    public function testCreateNewDBUpdatesTableIfNotExist()
    {
        $db = DB::get();

        $table = $this->removeDbUpdatesTable();

        new DBUpdates($db, new DBUpdatesMock());

        $this->assertTrue($table->tableExists());
    }

    public function testExecute2NewUpdates()
    {
        $db = DB::get();

        $this->removeDbUpdatesTable();
        $updates = new DBUpdatesMock();

        $dbUpdates = new DBUpdates($db, $updates);

        $int_anzahl_abgearbeitete_updates = $dbUpdates->execute();

        $this->assertEquals(2, $int_anzahl_abgearbeitete_updates);
    }

    public function testExecute1NewOf2Updates()
    {
        $db = DB::get();

        $this->removeDbUpdatesTable();
        $updates = new DBUpdatesMock();

        $dbUpdates = new DBUpdates($db, $updates);

        $insert = $db->prepareInsert(DBUpdates::$tableDbUpdates);
        $insert->setNewPKValue('dbupdate_id', 10);
        $insert->setNewValue('dbupdate_execution_status', 'skipped');
        $insert->setNewValue('dbupdate_execution_date', '');
        $insert->setNewValue('dbupdate_execution_duration', 0);
        $insert->setNewValue('dbupdate_description', 'Simulates an existing update with ID #10');
        $insert->setNewValue('dbupdate_execution_feedback', '');
        $insert->insert();

        $int_anzahl_abgearbeitete_updates = $dbUpdates->execute();

        $this->assertEquals(1, $int_anzahl_abgearbeitete_updates);
    }

    public function testExecute1NewSuspendedOf2Updates()
    {
        $db = DB::get();

        $this->removeDbUpdatesTable();
        $updates = new DBUpdatesMock();

        $dbUpdates = new DBUpdates($db, $updates);

        $insert = $db->prepareInsert(DBUpdates::$tableDbUpdates);
        $insert->setNewPKValue('dbupdate_id', 10);
        $insert->setNewValue('dbupdate_execution_status', 'running');
        $insert->setNewValue('dbupdate_execution_date', '');
        $insert->setNewValue('dbupdate_execution_duration', 0);
        $insert->setNewValue('dbupdate_description', 'Simulates an existing running update with ID #10');
        $insert->setNewValue('dbupdate_execution_feedback', 'Example for most recent Feedback (See next line here)');
        $insert->insert();

        $int_anzahl_abgearbeitete_updates = $dbUpdates->execute();

        $this->assertEquals(2, $int_anzahl_abgearbeitete_updates);
    }

    public function testExecute1NewStillSuspendedOf2Updates()
    {
        $db = DB::get();

        $this->removeDbUpdatesTable();
        $updates = new DBUpdatesMock();

        // Overwrites the demo update
        $updates->updates[20]["description"] = "PHP Closure Test";
        $updates->updates[20]["operation_php"] = function (DB $db)
        {
            usleep(40);
            throw new DBUpdatesSuspendJobException('Another chunk processed...', DBUpdatesSuspendJobException::MODE_SUSPEND_OTHERS);
        };

        $dbUpdates = new DBUpdates($db, $updates);

        $insert = $db->prepareInsert(DBUpdates::$tableDbUpdates);
        $insert->setNewPKValue('dbupdate_id', 20);
        $insert->setNewValue('dbupdate_execution_status', 'running');
        $insert->setNewValue('dbupdate_execution_date', '');
        $insert->setNewValue('dbupdate_execution_duration', 0);
        $insert->setNewValue('dbupdate_description', 'Simulates an existing running update with ID #10');
        $insert->setNewValue('dbupdate_execution_feedback', 'Example for most recent Feedback (See next line here)');
        $insert->insert();

        $int_anzahl_abgearbeitete_updates = $dbUpdates->execute();

        $this->assertEquals(2, $int_anzahl_abgearbeitete_updates);
    }


    protected function tearDown()
    {
//        $table = new DBTable(DB::get(), DBUpdates::$tableDbUpdates);
//        $table->removeTable();
    }


}
 