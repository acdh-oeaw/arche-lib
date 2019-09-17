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

use zozlak\RdfConstants as C;

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
            C::RDF_TYPE                  => ['https://class/1', 'https://class/2'],
            self::$config->schema->id    => ['https://an.unique.id/1', 'https://an.unique.id/2'],
            self::$config->schema->label => 'sample label for the first resource',
            'https://date.prop'          => '2019-01-01',
            'https://number.prop'        => 150,
            'https://lorem.ipsum'        => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed iaculis nisl enim, malesuada tempus nisl ultrices ut. Duis egestas at arcu in blandit. Nulla eget sem urna. Sed hendrerit enim ut ultrices luctus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est.',
        ]);
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);
        self::$repo->commit();
    }

    /**
     * @group RepoResource
     */
    public function testGetClasses() {
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
    public function testGetIds() {
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $ids = $res->getIds();
        $this->assertEquals(3, count($ids));
        foreach (['https://an.unique.id/1', 'https://an.unique.id/2'] as $i) {
            $this->assertTrue(in_array($i, $ids));
        }
    }

    public function testHasBinaryContent() {
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $this->assertFalse($res->hasBinaryContent());
        
        self::$repo->begin();
        $content = new BinaryPayload(__FILE__, null, 'text/plain');
        $res->updateContent($content);
        self::$repo->commit();
        
        $this->assertTrue($res->hasBinaryContent());
    }
    
    public function testGetContent() {
        $res = self::$repo->getResourceById('https://an.unique.id/1');
        $this->assertFalse($res->hasBinaryContent());
        
        self::$repo->begin();
        $content = new BinaryPayload('sample content', '/dummy/path', 'text/plain');
        $res->updateContent($content);
        self::$repo->commit();
        
        $this->assertEquals('sample content', $res->getContent()->getBody());
    }
    
}
