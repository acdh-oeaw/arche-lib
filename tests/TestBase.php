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

namespace acdhOeaw\acdhRepoLib;

use EasyRdf\Graph;
use EasyRdf\Resource;
use GuzzleHttp\Exception\ClientException;
use acdhOeaw\acdhRepoLib\exception\Deleted;
use acdhOeaw\acdhRepoLib\exception\NotFound;

/**
 * Description of TestBase
 *
 * @author zozlak
 */
class TestBase extends \PHPUnit\Framework\TestCase {

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    static protected $repo;
    static protected $config;

    static public function setUpBeforeClass(): void {
        $cfgFile      = __DIR__ . '/config.yaml';
        self::$config = json_decode(json_encode(yaml_parse_file($cfgFile)));
        self::$repo   = Repo::factory($cfgFile);
    }

    static public function tearDownAfterClass(): void {
        
    }

    private $resources;

    public function setUp(): void {
        $this->resources = [];
    }

    public function tearDown(): void {
        try {
            self::$repo->rollback();
        } catch (ClientException $e) {
            
        }
        self::$repo->begin();
        foreach ($this->resources as $i) {
            try {
                $i->delete(true, true);
            } catch (Deleted $e) {
                
            } catch (NotFound $e) {
                
            }
        }
        self::$repo->commit();
    }

    protected function noteResource(RepoResource $res) {
        $this->resources[] = $res;
    }

    protected function getMetadata(array $properties): Resource {
        $graph = new Graph();
        $res   = $graph->newBNode();
        foreach ($properties as $p => $v) {
            switch ($p) {
                case 'id':
                    $p = self::$config->schema->id;
                    break;
                case 'rel':
                    $p = self::$config->schema->parent;
                    break;
                case 'title':
                case 'label':
                    $p = self::$config->schema->label;
                    break;
            }
            if (!is_array($v)) {
                $v = [$v];
            }
            foreach ($v as $i) {
                if (preg_match('|^https?://|', $i)) {
                    $res->addResource($p, $i);
                } else {
                    $res->addLiteral($p, $i);
                }
            }
        }
        return $res;
    }

}
