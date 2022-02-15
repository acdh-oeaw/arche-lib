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

use EasyRdf\Literal;
use GuzzleHttp\Exception\ClientException;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\ExceptionUtil;

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

    public function testCreateBinaryResource(): void {
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

    public function testCreateResource(): void {
        $labelProp = self::$schema->label;
        $metadata  = $this->getMetadata([$labelProp => 'sampleTitle']);

        self::$repo->begin();
        $res1 = self::$repo->createResource($metadata);
        $this->noteResource($res1);
        $this->assertEquals('sampleTitle', (string) $res1->getMetadata()->getLiteral($labelProp));
        self::$repo->commit();

        $res2 = new RepoResource($res1->getUri(), self::$repo);
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

    public function testMap(): void {
        $prop    = 'http://foo/bar';
        $valueOk = rand();
        $metaOk  = $this->getMetadata([$prop => $valueOk]);
        $metaBad = $this->getMetadata([$prop => new Literal('baz', null, RDF::XSD_DATE)]);
        self::$repo->begin();

        // REJEST_SKIP
        $results = self::$repo->map([$metaOk, $metaBad], fn($meta) => self::$repo->createResourceAsync($meta), 1, Repo::REJECT_SKIP);
        $this->assertCount(1, $results);
        $this->assertInstanceOf(RepoResource::class, $results[0]);
        $this->assertEquals($valueOk, (string) $results[0]->getGraph()->get($prop));

        // REJECT_INCLUDE
        $results   = self::$repo->map([$metaOk, $metaBad], fn($meta) => self::$repo->createResourceAsync($meta), 1, Repo::REJECT_INCLUDE);
        $this->assertCount(2, $results);
        $rejected  = $fulfilled = null;
        foreach ($results as $i) {
            if ($i instanceof RepoResource) {
                $fulfilled = $i;
            } elseif ($i instanceof ClientException) {
                $rejected = $i;
            }
        }
        $this->assertNotNull($rejected);
        $this->assertNotNull($fulfilled);
        $this->assertEquals($valueOk, (string) $fulfilled->getGraph()->get($prop));
        $this->assertEquals(400, $rejected->getResponse()->getStatusCode());
        $this->assertStringContainsString('Wrong property value', (string) $rejected->getResponse()->getBody());

        // REJECT_FAIL
        try {
            $results = self::$repo->map([$metaOk, $metaBad], fn($meta) => self::$repo->createResourceAsync($meta), 1, Repo::REJECT_FAIL);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getResponse()->getStatusCode());
            $this->assertStringContainsString('Wrong property value', (string) $e->getResponse()->getBody());
            $unwrapped = ExceptionUtil::unwrap($e, false);
            $this->assertStringContainsString('HTTP 400 with message:', $unwrapped);
            $this->assertStringContainsString('Wrong property value', $unwrapped);
        }

        self::$repo->rollback();
    }

    public function testFactoryFromUrl(): void {
        self::$repo->begin();
        $resUrl  = self::$repo->createResource($this->getMetadata([]))->getUri();
        self::$repo->rollback();
        $baseUrl = self::$repo->getBaseUrl();

        $repos = [
            Repo::factoryFromUrl($baseUrl),
            Repo::factoryFromUrl($baseUrl . "describe"),
            Repo::factoryFromUrl("$baseUrl/transaction"),
            Repo::factoryFromUrl($resUrl),
            Repo::factoryFromUrl("$resUrl/metadata"),
        ];
        foreach ($repos as $i) {
            $this->assertInstanceOf(Repo::class, $i);
        }
    }

    public function testFactoryInteractive(): void {
        $baseUrl = self::$repo->getBaseUrl();
        $output  = $result  = null;
        $baseCmd = "php -f " . escapeshellarg(__DIR__ . "/factoryInteractive.php");

        // OK
        $stdIn = [
            "$baseUrl\\nyes\\nuser\\npassword\\n",
            "$baseUrl\\nyes\\n",
            "1\\nyes\\nuser\\npassword\\n",
            "1\\nyes\\nuser\\n",
            "1\\nyes\\n",
        ];
        $param = [
            "",
            "",
            __DIR__ . "/config.yaml",
            __DIR__ . "/config.yaml user",
            __DIR__ . "/config.yaml user password",
        ];
        for ($i = 0; $i < count($stdIn); $i++) {
            exec("echo " . escapeshellarg($stdIn[$i]) . " | $baseCmd $param[$i] 2>&1", $output, $result);
            $this->assertEquals(0, $result, "Repo initialization test $i failed");
        }

        // FAIL
        $stdIn = [
            "$baseUrl/foo\\nyes\\n",
            "2\\nyes\\n",
            "1\\nfoo\\n",
        ];
        $param = [
            "",
            __DIR__ . "/config.yaml",
            __DIR__ . "/config.yaml",
        ];
        for ($i = 0; $i < count($stdIn); $i++) {
            exec("echo -e " . escapeshellarg($stdIn[$i]) . " | $baseCmd $param[$i] 2>&1", $output, $result);
            $this->assertGreaterThan(0, $result, "Repo initialization fail test $i succeeded");
        }
    }
}
