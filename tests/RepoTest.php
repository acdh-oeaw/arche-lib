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

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\TransferStats;
use zozlak\RdfConstants as RDF;
use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\ExceptionUtil;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\exception\TooManyRequests;

/**
 * Description of RepoTest
 *
 * @author zozlak
 */
class RepoTest extends TestBase {

    public function testCreateFromConfig(): void {
        $repo = Repo::factory(__DIR__ . '/config.yaml');
        $this->assertInstanceOf(Repo::class, $repo);
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
        $labelTmpl = new PT($labelProp);
        $metadata  = $this->getMetadata(['label' => 'sampleTitle']);
        $binary    = new BinaryPayload(null, __FILE__);

        self::$repo->begin();
        $res1 = self::$repo->createResource($metadata, $binary);
        $this->assertEquals(file_get_contents(__FILE__), (string) $res1->getContent()->getBody(), 'file content mismatch');
        $this->assertEquals('sampleTitle', $res1->getMetadata()->getObjectValue($labelTmpl));
        self::$repo->commit();

        $res2 = new RepoResource($res1->getUri(), self::$repo);
        $this->assertEquals(file_get_contents(__FILE__), (string) $res2->getContent()->getBody(), 'file content mismatch');
        $this->assertEquals('sampleTitle', $res2->getMetadata()->GetObjectValue($labelTmpl));
    }

    public function testCreateResource(): void {
        $labelProp = self::$schema->label;
        $labelTmpl = new PT($labelProp);
        $metadata  = $this->getMetadata(['label' => 'sampleTitle']);

        self::$repo->begin();
        $res1 = self::$repo->createResource($metadata);
        $this->assertEquals('sampleTitle', (string) $res1->getMetadata()->GetObjectValue($labelTmpl));
        self::$repo->commit();

        $res2 = new RepoResource($res1->getUri(), self::$repo);
        $this->assertEquals('sampleTitle', (string) $res2->getMetadata()->GetObjectValue($labelTmpl));
    }

    public function testCreateDuplicate(): void {
        $id       = 'http://my.id';
        $metadata = $this->getMetadata(['id' => $id]);

        self::$repo->begin();
        $res = self::$repo->createResource($metadata);
        try {
            self::$repo->createResource($metadata);
            /** @phpstan-ignore method.impossibleType */
            $this->assertTrue(false);
        } catch (Conflict $e) {
            $this->assertEquals(409, $e->getCode());
            $this->assertEquals("Duplicated resource identifier: $id", $e->getMessage());
            $this->assertEquals((string) $res->getUri(), $e->getExistingUri());
        }
        self::$repo->rollback();
    }

    public function testSearchById(): void {
        $idProp = self::$schema->id;
        $id     = 'https://a.b/' . rand();
        $meta   = $this->getMetadata(['id' => $id]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta);
        self::$repo->commit();

        $res2 = self::$repo->getResourceById($id);
        $this->assertEquals($res1->getUri(), $res2->getUri());
        $meta = [
            'getGraph()'    => $res2->getGraph(),
            'getMetadata()' => $res2->getMetadata(),
        ];
        foreach ($meta as $k => $v) {
            $this->assertTrue($v->none(new PT(self::$schema->searchMatch)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchOrder)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchOrderValue)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFts)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFtsProperty)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFtsQuery)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchMatch)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchOrder)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchOrderValue)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFts)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFtsProperty)), $k);
            $this->assertTrue($v->none(new PT(self::$schema->searchFtsQuery)), $k);
        }
    }

    public function testSearchByIdNotFound(): void {
        $this->expectException(NotFound::class);
        self::$repo->getResourceById('https://no.such/id');
    }

    public function testSearchByIdsAmigous(): void {
        $idProp = self::$schema->id;
        $id1    = 'https://a.b/' . rand();
        $id2    = 'https://a.b/' . rand();
        $meta1  = $this->getMetadata(['id' => $id1]);
        $meta2  = $this->getMetadata(['id' => $id2]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta1);
        $res2   = self::$repo->createResource($meta2);
        self::$repo->commit();

        $this->expectException(AmbiguousMatch::class);
        self::$repo->getResourceByIds([$id1, $id2]);
    }

    public function testMap(): void {
        $prop     = 'http://foo/bar';
        $propTmpl = new PT($prop);
        $valueOk  = rand();
        $metaOk   = $this->getMetadata([$prop => $valueOk]);
        $metaBad  = $this->getMetadata([$prop => DF::literal('baz', null, RDF::XSD_DATE)]);
        self::$repo->begin();

        // REJEST_SKIP
        $results = self::$repo->map([$metaOk, $metaBad], fn($meta) => self::$repo->createResourceAsync($meta), 1, Repo::REJECT_SKIP);
        $this->assertCount(1, $results);
        $this->assertInstanceOf(RepoResource::class, $results[0]);
        $this->assertEquals($valueOk, (string) $results[0]->getGraph()->getObjectValue($propTmpl));

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
        $this->assertEquals($valueOk, (string) $fulfilled->getGraph()->getObjectValue($propTmpl));
        $this->assertEquals(400, $rejected->getResponse()->getStatusCode());
        $this->assertStringContainsString('Wrong property value', (string) $rejected->getResponse()->getBody());

        // REJECT_FAIL
        try {
            $results = self::$repo->map([$metaOk, $metaBad], fn($meta) => self::$repo->createResourceAsync($meta), 1, Repo::REJECT_FAIL);
            /** @phpstan-ignore method.impossibleType */
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

    public function testSkipTombstone(): void {
        $idProp = self::$schema->id;
        $id1    = 'https://a.b/' . rand();
        $id2    = 'https://a.b/' . rand();
        $meta1  = $this->getMetadata(['id' => $id1]);
        $meta2  = $this->getMetadata(['id' => $id2]);
        self::$repo->begin();
        $res1   = self::$repo->createResource($meta1);
        $res2   = self::$repo->createResource($meta2);
        self::$repo->commit();

        $query     = "SELECT id FROM identifiers WHERE ids IN (?, ?)";
        $resources = self::$repo->getResourcesBySqlQuery($query, [$id1, $id2], new SearchConfig());
        $resources = array_map(fn($x) => (string) $x->getUri(), iterator_to_array($resources));
        $this->assertCount(2, $resources);
        $this->assertContains((string) $res1->getUri(), $resources);
        $this->assertContains((string) $res2->getUri(), $resources);

        self::$repo->begin();
        $res2->delete(false);
        self::$repo->commit();
        $client = new Client(['http_errors' => false]);
        $resp   = $client->sendRequest(new Request('get', (string) $res2->getUri()));
        $this->assertEquals(410, $resp->getStatusCode());

        $query     = "SELECT id FROM identifiers WHERE ids IN (?, ?)";
        $resources = self::$repo->getResourcesBySqlQuery($query, [$id1, $id2], new SearchConfig());
        $resources = iterator_to_array($resources);
        $this->assertCount(1, $resources);
        $this->assertEquals((string) $res1->getUri(), (string) $resources[0]->getUri());
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

    /**
     * Tests a scenario where the base URL used to make HTTP requests differs
     * from the repository base URL (e.g. when we want to ingest directly against
     * the repository instance which is normally reachable trough a proxy).
     */
    public function testDifferentBaseUrlUri(): void {
        $guzzleOpts = [
            'headers'  => ['eppn' => 'admin'],
            'on_stats' => fn(TransferStats $s) => $this->assertStringStartsWith('http://localhost/api/', (string) $s->getEffectiveUri()),
        ];
        $repo       = Repo::factoryFromUrl('http://localhost/api/', $guzzleOpts);
        $this->assertEquals('http://127.0.0.1/api/', $repo->getBaseUrl());

        $repo->begin();
        $res = $repo->createResource($this->getMetadata([]));
        $this->assertStringStartsWith('http://127.0.0.1/api/', (string) $res->getUri());
        $res->setMetadata($res->getMetadata()); // just to invalidate local copy
        $res->updateMetadata();
        $res->updateContent(new BinaryPayload(null, __FILE__));
        $repo->rollback();
    }

    public function testTooManyRequests(): void {
        $conn = $this->saturateDbConnections();
        try {
            self::$repo->getResourceById('http://foo/bar');
            /** @phpstan-ignore method.impossibleType */
            $this->assertTrue(false);
        } catch (TooManyRequests $e) {
            /** @phpstan-ignore method.alreadyNarrowedType */
            $this->assertTrue(true);
        }
    }
}
