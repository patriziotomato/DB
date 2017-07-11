<?php

namespace tests\classes;

use ScoopDb\DB;
use ScoopDb\DBTable;
use ScoopDb\DBTableColumn;

error_reporting(E_ALL ^ E_NOTICE);

define('DB_HOST', "db2.scoop-medianet.de");
define('DB_DATABASE', "scoop_db");
define('DB_USER', "scoop_db");
define('DB_PWD', "aZTXZ2WT");

require_once __DIR__.'/../../vendor/autoload.php';
require_once __DIR__.'/../../src/ScoopDb/DB.php';


class DBTest extends \PHPUnit_Framework_TestCase
{
    public static $testTable = 'test_unit';

    public static function setUpBeforeClass()
    {
        $db = DB::get();

        $dbTable = new DBTable($db, static::$testTable);

        if ($dbTable->tableExists()) {
            $dbTable->removeTable();
        }

        $dbTable->addColumn(
            $fieldName = 'id',
            $fieldType = DBTableColumn::$fieldTypes['INT'],
            $fieldLength = 11,
            $fieldCanBeNull = false,
            $fieldIsPK = true,
            $fieldIsAutoincrement = true
        );

        $dbTable->addColumn(
            $fieldName = 'test_null',
            $fieldType = DBTableColumn::$fieldTypes['VARCHAR'],
            $fieldLength = 200,
            $fieldCanBeNull = true,
            $fieldIsPK = false,
            $fieldIsAutoincrement = false
        );

        $dbTable->addColumn(
            $fieldName = 'my_time_column',
            $fieldType = DBTableColumn::$fieldTypes['DATETIME'],
            $fieldLength = null,
            $fieldCanBeNull = false,
            $fieldIsPK = false,
            $fieldIsAutoincrement = false
        );

        $dbTable->createTable();
    }

    public function testInsertSQL()
    {
        $db = DB::get();

        $desiredResult = 'INSERT INTO ' . static::$testTable . "\nSET `test_unit`.`id` = 1";

        $sql = $db->prepareInsertWithoutPK(static::$testTable, 'id')
            ->setNewPKValue('id', 1)
            ->insert(true);

        $this->assertEquals(trim($desiredResult), trim($sql));
    }

    public function testInsert()
    {
        $db = DB::get();

        $db->truncateTable(static::$testTable);

        $insert = $db->prepareInsert(static::$testTable);

        for ($i = 1; $i <= 10; $i++) {
//            $insert->setNewPKValue('id', $i);
            $insert->setNewValue('test_null', $i % 2 == 0 ? null : 1);
            $insert->insert();
        }

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(10, $cnt);
    }

    public function testInsert2()
    {
        $db = DB::get();

        $db->truncateTable(static::$testTable);

        $insert = $db->prepareInsertWithoutPK(static::$testTable, 'id');

        for ($i = 1; $i <= 10; $i++) {
            $insert->setNewPKValue('id', $i);
            $insert->setNewValue('test_null', $i % 2 == 0 ? null : 1);
            $insert->insert();
        }

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(10, $cnt);
    }

    public function testInsertIgnore()
    {
        $db = DB::get();

        $insert = $db->prepareInsertWithoutPK(static::$testTable, 'id');

        $insert->setNewPKValue('id', 1);
        $insert->setNewValue('test_null', null);

        $insert->insert(false, true);

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(10, $cnt);
    }

    public function testInsertIgnore2()
    {
        $db = DB::get();

        $insert = $db->prepareInsert(static::$testTable);

        $insert->setNewPKValue('id', 1);
        $insert->setNewValue('test_null', null);

        $insert->insert(false, true);

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(10, $cnt);
    }

    public function testInsertOrUpdate() {
        $db = DB::get();

        $insert = $db->prepareInsert(static::$testTable);

        $insert->setNewPKValue('id', 5);
        $insert->setNewValue('test_null', 2);

        $insert->insertOrUpdate();

        $valueInTestNull = $db->select(static::$testTable)
            ->constraintEquals('id', 5)
            ->fetchCell('test_null');

        $this->assertEquals(2, $valueInTestNull);
    }

    public function testDelete1()
    {
        $db = DB::get();

        $select = $db->select(static::$testTable)->constraintEquals('id', 1);

        $db->delete($select);

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(9, $cnt);
    }

    public function testDelete2()
    {
        $db = DB::get();

        $select = $db->select(static::$testTable)->constraintIn('id', array(3, 4));

        $db->delete($select);

        $cnt = $db->countSelect($db->select(static::$testTable));
        $this->assertEquals(7, $cnt);
    }

    public function testUpdate1()
    {
        $db = DB::get();

        // Perform update
        $db->prepareUpdate(static::$testTable, 10)
            ->setNewPKValue('id', 20)->update();

        // Check if there is a 20 in DB
        $cnt = $db->countSelect($db->select(static::$testTable)->constraintEquals('id', 20));

        $this->assertEquals(1, $cnt);


        $db->prepareUpdate(static::$testTable, 7)
            ->setNewValue('my_time_column', '2013-01-01 00:00:00')
            ->update();
        $db->prepareUpdate(static::$testTable, 8)
            ->setNewValue('my_time_column', '2013-12-31 23:59:59')
            ->update();
        $db->prepareUpdate(static::$testTable, 9)
            ->setNewValue('my_time_column', '2014-01-01 00:00:00')
            ->update();
    }

    public function testContraintIsNull()
    {
        $db = DB::get();

        $cntNull = $db->select(static::$testTable)
            ->constraintIsNull('test_null')
            ->count();

        $this->assertEquals(4, $cntNull);
    }

    public function testContraintIsNotNull()
    {
        $db = DB::get();

        $cntNull = $db->select(static::$testTable)
            ->constraintIsNotNull('test_null')
            ->count();

        $this->assertEquals(3, $cntNull);
    }


    public function testContraintIn()
    {
        $db = DB::get();

        $cntIn = $db->select(static::$testTable)
            ->constraintIn('id', array(5, '9'))
            ->count();

        $this->assertEquals(2, $cntIn);

        $cntIn = $db->select(static::$testTable)
            ->constraintIn('id', '9')
            ->count();

        $this->assertEquals(1, $cntIn);

        $cntIn = $db->select(static::$testTable)
            ->constraintIn('id', '1')
            ->count();

        $this->assertEquals(0, $cntIn);

        $cntIn = $db->select(static::$testTable)
            ->constraintIn('test_null', 1)
            ->count();

        $this->assertEquals(2, $cntIn);

        // TODO: This method should throw an warning to use constraintIsNull instead!
        $this->assertEquals(
            0,
            $db->select(static::$testTable)
                ->constraintIn('test_null', null)
                ->count()
        );

        // TODO: This method should throw an warning not to use a null value within an constraintIn()!
        $this->assertEquals(
            2,
            $db->select(static::$testTable)
                ->constraintIn('test_null', array('1', null))
                ->count()
        );

//        $this->assertEquals('SELECT * FROM ...', $selectInWithNull);
    }

    public function testConstraintBetween()
    {
        $db = DB::get();

        $select = $db->select(static::$testTable)
            ->constraintBetween('id', 5, 8);

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`id` BETWEEN 5 AND 8",
            $select->sql()
        );
        $this->assertEquals(4, $select->count());

        $select = $db->select(static::$testTable)
            ->constraintBetween('test_null', 1, 'Z');

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`test_null` BETWEEN '1' AND 'Z'",
            $select->sql()
        );

        $select = $db->select(static::$testTable)
            ->constraintBetween('my_time_column', '2013-01-01 00:00:00', '2013-12-31 23:59:59');

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`my_time_column` BETWEEN '2013-01-01 00:00:00' AND '2013-12-31 23:59:59'",
            $select->sql()
        );
        $this->assertEquals(2, $select->count());
    }

    public function testConstraintNotBetween()
    {
        $db = DB::get();

        $select = $db->select(static::$testTable)
            ->constraintNotBetween('id', 5, 8);

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`id` NOT BETWEEN 5 AND 8",
            $select->sql()
        );
        $this->assertEquals(3, $select->count());

        $select = $db->select(static::$testTable)
            ->constraintNotBetween('test_null', 1, 'Z');

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`test_null` NOT BETWEEN '1' AND 'Z'",
            $select->sql()
        );

        $select = $db->select(static::$testTable)
            ->constraintNotBetween('my_time_column', '2013-01-01 00:00:00', '2013-12-31 23:59:59');

        $this->assertEquals(
            "SELECT *\nFROM " . static::$testTable . "\nWHERE `" . static::$testTable . "`.`my_time_column` NOT BETWEEN '2013-01-01 00:00:00' AND '2013-12-31 23:59:59'",
            $select->sql()
        );
        $this->assertEquals(5, $select->count());
    }

    public function testTableDelete()
    {
        $db = DB::get();

        $dbTable = new DBTable($db, static::$testTable);
        $dbTable->removeTable();

        $this->assertFalse($dbTable->tableExists());
    }

    public static function tearDownAfterClass()
    {
        $db = DB::get();
    }
}
 