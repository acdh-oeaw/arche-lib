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

use acdhOeaw\acdhRepoLib\exception\AmbiguousMatch;
use acdhOeaw\acdhRepoLib\exception\Deleted;
use acdhOeaw\acdhRepoLib\exception\NotFound;

/**
 * Description of RepoTest
 *
 * @author zozlak
 */
class RepoTest extends TestBase {

    public function testCreateFromConfig(): void {
        $repo = Repo::factory(__DIR__ . '/config.yaml');
        $this->assertTrue(is_a($repo, 'acdhOeaw\acdhRepoLib\Repo'));
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
    public function testTransactionProlong() {
        self::$repo->begin();
        sleep(self::$config->transactionController->timeout - 2);
        self::$repo->prolong();
        sleep(3);
        self::$repo->commit();
        $this->assertFalse(self::$repo->inTransaction());
    }

    /**
     * @large
     */
    public function testTransactionExpired() {
        self::$repo->begin();
        sleep(self::$config->transactionController->timeout + 1);
        $this->expectException('GuzzleHttp\Exception\ClientException');
        $this->expectExceptionMessage('resulted in a `400 Bad Request` response');
        self::$repo->commit();
    }

    public function testCreateResource() {
        $labelProp = self::$config->schema->label;
        $metadata  = $this->getMetadata([$labelProp => 'sampleTitle']);
        $binary    = new BinaryPayload(null, __FILE__);

        self::$repo->begin();
        $res1 = self::$repo->createResource($metadata, $binary);
        $this->noteResource($res1);
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

    public function testSearchById() {
        $idProp = self::$config->schema->id;
        $id     = 'https://a.b/' . rand();
        $meta   = $this->getMetadata([$idProp => $id]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta);
        $this->noteResource($res1);
        self::$repo->commit();

        $res2 = self::$repo->getResourceById($id);
        $this->assertEquals($res1->getUri(), $res2->getUri());
    }

    public function testSearchByIdNotFound() {
        $this->expectException(NotFound::class);
        self::$repo->getResourceById('https://no.such/id');
    }
    
    public function testSearchByIdsAmigous() {
        $idProp = self::$config->schema->id;
        $id1     = 'https://a.b/' . rand();
        $id2     = 'https://a.b/' . rand();
        $meta1   = $this->getMetadata([$idProp => $id1]);
        $meta2   = $this->getMetadata([$idProp => $id2]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta1);
        $this->noteResource($res1);
        $res2   = self::$repo->createResource($meta2);
        $this->noteResource($res2);
        self::$repo->commit();

        $this->expectException(AmbiguousMatch::class);
        self::$repo->getResourceByIds([$id1, $id2]);
    }
    
    public function testDeleteResource() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $res1->delete(false, false);

        // it should succeed cause tombstones can be still referenced
        self::$repo->commit();

        $this->assertEquals($res1->getUri(), (string) $res2->getMetadata()->getResource($relProp));
        $this->expectExceptionCode(410);
        $res1->loadMetadata(true);
    }

    public function testDeleteWithConflict() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2 = self::$repo->createResource($meta2);
        $this->noteResource($res2);

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
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $res1->delete(true, true);
        self::$repo->commit();

        $res2->loadMetadata(true);
        $this->assertNull($res2->getMetadata()->getResource($relProp));
        $this->expectExceptionCode(404);
        $res1->loadMetadata(true);
    }

    public function testDeleteRecursively() {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$config->schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $res1->deleteRecursively($relProp, false, false);
        self::$repo->commit();

        try {
            $res1->loadMetadata();
            $this->assertTrue(false, 'No exception');
        } catch (Deleted $e) {
            $this->assertEquals(410, $e->getCode());
        }
        try {
            $res2->loadMetadata(true);
            $this->assertTrue(false, 'No exception');
        } catch (Deleted $e) {
            $this->assertEquals(410, $e->getCode());
        }
    }

}
