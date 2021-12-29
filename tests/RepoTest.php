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

use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;

/**
 * Description of RepoTest
 *
 * @author zozlak
 */
class RepoTest extends TestBase {

    public function testCreateFromConfig(): void {
        $repo = Repo::factory(__DIR__ . '/config.yaml');
        $this->assertTrue(is_a($repo, 'acdhOeaw\arche\lib\Repo'));
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
    public function testTransactionProlong(): void {
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
    public function testTransactionExpired(): void {
        self::$repo->begin();
        sleep(self::$config->transactionController->timeout + 1);
        $this->expectException(\GuzzleHttp\Exception\ClientException::class);
        $this->expectExceptionMessageMatches('/resulted in a `400 Bad Request` response/');
        self::$repo->commit();
    }

    public function testCreateResource(): void {
        $labelProp = self::$schema->label;
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

    public function testSearchById(): void {
        $idProp = self::$schema->id;
        $id     = 'https://a.b/' . rand();
        $meta   = $this->getMetadata([$idProp => $id]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta);
        $this->noteResource($res1);
        self::$repo->commit();

        $res2 = self::$repo->getResourceById($id);
        $this->assertEquals($res1->getUri(), $res2->getUri());
    }

    public function testSearchByIdNotFound(): void {
        $this->expectException(NotFound::class);
        self::$repo->getResourceById('https://no.such/id');
    }

    public function testSearchByIdsAmigous(): void {
        $idProp = self::$schema->id;
        $id1    = 'https://a.b/' . rand();
        $id2    = 'https://a.b/' . rand();
        $meta1  = $this->getMetadata([$idProp => $id1]);
        $meta2  = $this->getMetadata([$idProp => $id2]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta1);
        $this->noteResource($res1);
        $res2   = self::$repo->createResource($meta2);
        $this->noteResource($res2);
        self::$repo->commit();

        $this->expectException(AmbiguousMatch::class);
        self::$repo->getResourceByIds([$id1, $id2]);
    }

}
