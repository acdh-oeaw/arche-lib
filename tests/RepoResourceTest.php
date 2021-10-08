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

use zozlak\RdfConstants as C;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;

/**
 * Description of RepoResourceTest
 *
 * @author zozlak
 */
class RepoResourceTest extends TestBase {

    public function setUp(): void {
        parent::setUp();

        self::$repo->begin();
        $meta1 = $this->getMetadata([
            C::RDF_TYPE           => ['https://class/1', 'https://class/2'],
            self::$schema->id     => ['https://an.unique.id/1', 'https://an.unique.id/2'],
            self::$schema->label  => 'sample label for the first resource',
            'https://date.prop'   => '2019-01-01',
            'https://number.prop' => 150,
            'https://lorem.ipsum' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed iaculis nisl enim, malesuada tempus nisl ultrices ut. Duis egestas at arcu in blandit. Nulla eget sem urna. Sed hendrerit enim ut ultrices luctus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est.',
        ]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);
        self::$repo->commit();
    }

    /**
     * @group RepoResource
     */
    public function testGetClasses(): void {
        $res     = self::$repo->getResourceById('https://an.unique.id/1');
        $classes = $res->getClasses();
        $this->assertEquals(2, count($classes));
        foreach (['https://class/1', 'https://class/2'] as $i) {
            $this->assertTrue(in_array($i, $classes));
        }

        $this->assertTrue($res->isA('https://class/1'));
        $this->assertFalse($res->isA('https://class/10'));
    }

    /**
     * @group RepoResource
     */
    public function testGetIds(): void {
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $ids = $res->getIds();
        $this->assertEquals(3, count($ids));
        foreach (['https://an.unique.id/1', 'https://an.unique.id/2'] as $i) {
            $this->assertTrue(in_array($i, $ids));
        }
    }

    public function testHasBinaryContent(): void {
        /** @var RepoResource $res */
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $this->assertFalse($res->hasBinaryContent());

        self::$repo->begin();
        $content = new BinaryPayload(__FILE__, null, 'text/plain');
        $res->updateContent($content);
        self::$repo->commit();

        $this->assertTrue($res->hasBinaryContent());
    }

    public function testGetContent(): void {
        /** @var RepoResource $res */
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $this->assertFalse($res->hasBinaryContent());

        self::$repo->begin();
        $content = new BinaryPayload('sample content', '/dummy/path', 'text/plain');
        $res->updateContent($content);
        self::$repo->commit();

        $this->assertEquals('sample content', $res->getContent()->getBody());
    }

    public function testUpdateMetadata(): void {
        $p1   = 'http://my.prop/1';
        $p2   = 'http://my.prop/2';
        $p3   = 'http://my.prop/3';
        $p4   = 'http://my.prop/4';
        $pd   = self::$schema->delete;
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

    public function testDelete(): void {
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $res1->delete(false, false);
        self::$repo->commit();

        $this->expectExceptionCode(410);
        $res1->loadMetadata(true);
    }

    public function testDeleteTombstone(): void {
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $res1->delete(true, false);
        self::$repo->commit();

        $this->expectExceptionCode(404);
        $res1->loadMetadata(true);
    }

    public function testDeleteWithConflict(): void {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $this->expectExceptionCode(409);
        $res1->delete(true, false);

        self::$repo->commit();
    }

    public function testDeleteWithReferences(): void {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
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

    public function testDeleteRecursively(): void {
        $relProp = 'https://some.prop';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $res1->delete(false, false, $relProp);
        self::$repo->commit();

        try {
            $res1->loadMetadata(true);
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

    public function testDeleteComplex(): void {
        $relProp   = 'https://some.prop';
        $otherProp = 'http://for/bar';
        self::$repo->begin();

        $id    = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([self::$schema->id => $id]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([$relProp => $res1->getUri()]);
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

        $meta3 = $this->getMetadata([
            $otherProp => $res1->getUri(),
            $otherProp => $res2->getUri(),
        ]);
        $res3  = self::$repo->createResource($meta3);
        $this->noteResource($res3);

        $res1->delete(true, true, $relProp);
        self::$repo->commit();

        try {
            $res1->loadMetadata(true);
            $this->assertTrue(false, 'No exception');
        } catch (NotFound $e) {
            $this->assertEquals(404, $e->getCode());
        }
        try {
            $res2->loadMetadata(true);
            $this->assertTrue(false, 'No exception');
        } catch (NotFound $e) {
            $this->assertEquals(404, $e->getCode());
        }
        $res3->loadMetadata(true);
        $this->assertNull($res3->getMetadata()->getResource($otherProp));
    }

    public function testMerge(): void {
        $idProp = self::$schema->id;
        $prop1  = 'http://a/1';
        $prop2  = 'http://a/2';
        $prop3  = 'http://a/3';
        self::$repo->begin();

        $id1   = 'https://a.b/' . rand();
        $meta1 = $this->getMetadata([
            $idProp => $id1,
            $prop1  => 'foo1',
            $prop2  => 'bar1',
        ]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $id2     = 'https://a.b/' . rand();
        $meta2   = $this->getMetadata([
            $idProp => $id2,
            $prop2  => 'bar2',
            $prop3  => 'baz2',
        ]);
        $res2    = self::$repo->createResource($meta2);
        $res2url = $res2->getUri();
        $this->noteResource($res2);

        $res2->merge($id1);
        $meta = $res2->getMetadata();
        $this->assertEquals($res1->getUri(), $res2->getUri());
        $ids  = array_map(function ($x) {
            return $x->getUri();
        }, $meta->all($idProp));
        $this->assertContains($id1, $ids);
        $this->assertContains($id2, $ids);
        $this->assertCount(4, $ids);

        self::$repo->commit();
        $this->expectExceptionCode(404);
        (new RepoResource($res2url, self::$repo))->loadMetadata();
    }
}
