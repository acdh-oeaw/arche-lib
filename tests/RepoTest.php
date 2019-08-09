<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace acdhOeaw\acdhRepoLib;

use EasyRdf\Graph;
use EasyRdf\Resource;
use GuzzleHttp\Exception\RequestException;

/**
 * Description of RepoTest
 *
 * @author zozlak
 */
class RepoTest extends \PHPUnit\Framework\TestCase {

    static private $repo;
    static private $config;

    static public function setUpBeforeClass(): void {
        $cfgFile      = __DIR__ . '/../../rdbms/config.yaml';
        self::$config = json_decode(json_encode(yaml_parse_file($cfgFile)));
        self::$repo   = Repo::factory($cfgFile);
    }

    static public function tearDownAfterClass(): void {
        
    }

    public function testCrateFromConfig(): void {
        $this->assertTrue(is_a(self::$repo, 'acdhOeaw\acdhRepoLib\Repo'));
    }

    public function testTransactionCommit(): void {
        self::$repo->begin();
        $this->assertTrue(self::$repo->inTransaction());
        self::$repo->commit();
        $this->assertFalse(self::$repo->inTransaction());
    }

    public function testTransactionRollback(): void {
        self::$repo->begin();
        $this->assertTrue(self::$repo->inTransaction());
        self::$repo->rollback();
        $this->assertFalse(self::$repo->inTransaction());
    }

    /**
     * @large
     */
    /*
      public function testTransactionProlong() {
      self::$repo->begin();
      sleep(self::$config->transactionController->timeout - 2);
      self::$repo->prolong();
      sleep(3);
      self::$repo->commit();
      }
     */

    /**
     * @large
     */
    /*
      public function testTransactionExpired() {
      self::$repo->begin();
      sleep(self::$config->transactionController->timeout + 1);
      $this->expectException('GuzzleHttp\Exception\ClientException');
      $this->expectExceptionMessage('resulted in a `400 Bad Request` response');
      self::$repo->commit();
      }
     */

    public function testCreateResource() {
        $labelProp = self::$config->schema->label;
        $metadata  = $this->getMetadata([
            $labelProp => 'sampleTitle'
        ]);
        $binary    = new BinaryPayload(null, __FILE__);

        self::$repo->begin();
        $res1 = self::$repo->createResource($metadata, $binary);
        $this->assertEquals(file_get_contents(__FILE__), (string) $res1->getContent()->getBody(), 'file content mismatch');
        $this->assertEquals('sampleTitle', (string) $res1->getMetadata()->getLiteral($labelProp));
        self::$repo->commit();

        $res2 = new RepoResource($res1->getUri(), self::$repo);
        $this->assertEquals(file_get_contents(__FILE__), (string) $res2->getContent()->getBody(), 'file content mismatch');
        $this->assertEquals('sampleTitle', (string) $res2->getMetadata()->getLiteral($labelProp));
    }

    public function testUpdateMetadata() {
        $p1   = 'http://my.prop/1';
        $p2   = 'http://my.prop/2';
        $p3   = 'http://my.prop/3';
        $p4   = 'http://my.prop/4';
        $pd   = self::$config->schema->delete;
        $meta = $this->getMetadata([$p1 => 'v1', $p2 => 'v2', $p3 => 'v3']);
        self::$repo->begin();
        $res  = self::$repo->createResource($meta);
        $this->assertEquals('v1', (string) $res->getMetadata()->get($p1));
        $this->assertEquals('v2', (string) $res->getMetadata()->get($p2));
        $this->assertEquals('v3', (string) $res->getMetadata()->get($p3));

        $meta = $this->getMetadata([$p3 => 'v33', $p4 => 'v4', $pd => $p1]);
        $res->setMetadata($meta);
        $res->updateMetadata();
        $this->assertEquals(null, $res->getMetadata()->get($p1));
        $this->assertEquals('v2', (string) $res->getMetadata()->get($p2));
        $this->assertEquals('v33', (string) $res->getMetadata()->get($p3));
        $this->assertEquals('v4', (string) $res->getMetadata()->get($p4));

        self::$repo->rollback();
    }

    public function testSearchByIds() {
        $idProp = self::$config->schema->id;
        $id     = 'https://a.b/' . rand();
        $meta   = $this->getMetadata([$idProp => $id]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta);
        self::$repo->commit();

//        $res2 = self::$repo->getResourceById($id);
//        $this->assertEquals($res1->getUri(), $res2->getUri());

        self::$repo->begin();
        $res1->delete(true);
        self::$repo->commit();

        $this->assertTrue(true);
    }

    public function testDeleteResource() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);

        $res1->delete(false, false);

        // it should succeed cause tombstones can be still referenced
        self::$repo->commit();

        $this->assertEquals($res1->getUri(), (string) $res2->getMetadata()->getResource($relProp));
        $this->expectExceptionCode(410);
        $res1->getMetadata(true);
    }

    public function testDeleteWithConflict() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        self::$repo->createResource($meta2);

        $res1->delete(true, false);

        $this->expectExceptionCode(409);
        self::$repo->commit();
    }

    public function testDeleteWithReferences() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);

        $res1->delete(true, true);
        self::$repo->commit();

        $this->assertNull($res2->getMetadata(true)->getResource($relProp));
        $this->expectExceptionCode(404);
        $res1->getMetadata(true);
    }

    public function testDeleteRecursively() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);

        $res1->deleteRecursively($relProp, false, false);
        self::$repo->commit();

        try {
            $res1->getMetadata(true);
        } catch (RequestException $e) {
            $this->assertEquals(410, $e->getCode());
        }
        try {
            $res2->getMetadata(true);
        } catch (RequestException $e) {
            $this->assertEquals(410, $e->getCode());
        }
    }

    // HELPER FUNCTIONS

    private function getMetadata(array $properties): Resource {
        $graph = new Graph();
        $res   = $graph->newBNode();
        foreach ($properties as $p => $v) {
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
