<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\lib\tests;

use GuzzleHttp\Exception\ClientException;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use acdhOeaw\arche\lib\Config;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\Schema;

/**
 * Description of TestBase
 *
 * @author zozlak
 */
class TestBase extends \PHPUnit\Framework\TestCase {

    static protected Repo $repo;
    static protected Config $config;
    static protected Schema $schema;

    static public function setUpBeforeClass(): void {
        $cfgFile      = __DIR__ . '/config.yaml';
        self::$config = Config::fromYaml($cfgFile);
        self::$repo   = Repo::factory($cfgFile);
        self::$schema = self::$repo->getSchema();
    }

    static public function tearDownAfterClass(): void {
        
    }

    public function setUp(): void {
        exec("docker exec -u www-data arche psql -c 'TRUNCATE resources CASCADE' 2>&1 > /dev/null");
        if (file_exists(__DIR__ . '/../log/rest.log')) {
            unlink(__DIR__ . '/../log/rest.log');
        }
        if (file_exists(__DIR__ . '/../log/tx.log')) {
            unlink(__DIR__ . '/../log/tx.log');
        }
    }

    public function tearDown(): void {
        if (self::$repo->inTransaction()) {
            try {
                self::$repo->rollback();
            } catch (ClientException) {
                
            }
        }
    }

    /**
     * 
     * @param array<string, mixed> $properties
     * @return DatasetNode
     */
    protected function getMetadata(array $properties): DatasetNode {
        $res   = DF::blankNode();
        $graph = new DatasetNode($res);
        foreach ($properties as $p => $v) {
            switch ($p) {
                case 'id':
                    $p = self::$schema->id;
                    break;
                case 'rel':
                case 'parent':
                    $p = self::$schema->parent;
                    break;
                case 'title':
                case 'label':
                    $p = self::$schema->label;
                    break;
                default:
                    $p = DF::namedNode($p);
            }
            if (!is_array($v)) {
                $v = [$v];
            }
            foreach ($v as $i) {
                if (!is_object($i)) {
                    $i = preg_match('|^https?://|', $i) ? DF::namedNode($i) : DF::literal($i);
                }
                $graph->add(DF::quad($res, $p, $i));
            }
        }
        return $graph;
    }
}
