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

use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use termTemplates\QuadTemplate as QT;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\SearchTerm;
use acdhOeaw\arche\lib\exception\TooManyRequests;
use zozlak\RdfConstants as RDF;

/**
 * Description of SearchTest
 *
 * @author zozlak
 */
class SearchTest extends TestBase {

    public function setUp(): void {
        parent::setUp();

        self::$repo->begin();

        $meta1 = $this->getMetadata([
            'id'                  => 'https://an.unique.id',
            'label'               => 'sample label for the first resource',
            'https://number.prop' => 150,
            'https://lorem.ipsum' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed iaculis nisl enim, malesuada tempus nisl ultrices ut. Duis egestas at arcu in blandit. Nulla eget sem urna. Sed hendrerit enim ut ultrices luctus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est.',
            'https://date.prop'   => DF::literal('2019-01-01', null, RDF::XSD_DATE),
        ]);
        $res1  = self::$repo->createResource($meta1);

        $meta2 = $this->getMetadata([
            'id'                  => 'https://res2.id',
            'parent'              => $res1->getUri(),
            'label'               => 'a more original title for a resource',
            'https://number.prop' => 20,
            'https://lorem.ipsum' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est. Aenean eleifend ipsum eu placerat sagittis. Aenean ullamcorper dignissim enim, ut congue turpis tristique eu.',
            'https://date.prop'   => DF::literal('2019-02-01', null, RDF::XSD_DATE),
        ]);
        self::$repo->createResource($meta2);

        self::$repo->commit();
    }

    /**
     * @group search
     */
    public function testSearchBySqlQuery(): void {
        $query  = "SELECT id FROM metadata WHERE property = ? AND value LIKE '%original%'";
        $param  = [self::$schema->label];
        $result = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getObjectValue(new QT(predicate: self::$schema->label)));
    }

    /**
     * @group search
     */
    public function testSearchBySqlQueryEmpty(): void {
        $param  = [];
        $result = self::$repo->getResourcesBySqlQuery("SELECT -1 AS id WHERE false", $param, new SearchConfig());
        $this->assertEquals([], iterator_to_array($result));
    }

    /**
     * @group search
     */
    public function testSearchBySearchTerms(): void {
        $terms  = [
            new SearchTerm('https://number.prop', 30, '<=', RDF::XSD_DECIMAL),
            new SearchTerm('https://date.prop', '2019-01-15', '>=', RDF::XSD_DATE),
            new SearchTerm('https://lorem.ipsum', 'ipsum', '@@'),
        ];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getObjectValue(new QT(predicate: self::$schema->label)));
    }

    /**
     * @group search
     */
    public function testSearchWrongDataType(): void {
        $term1  = new SearchTerm('https://number.prop', 30, '<=', RDF::XSD_STRING);
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms([$term1], new SearchConfig()));
        $this->assertEquals(2, count($result));
    }

    /**
     * @group search
     */
    public function testSearchFtsHighlight(): void {
        $ftsValueTmpl = new QT(predicate: self::$schema->searchFts->getValue() . '1');
        $ftsPropTmpl  = new QT(predicate: self::$schema->searchFtsProperty->getValue() . '1');
        $ftsQueryTmpl = new QT(predicate: self::$schema->searchFtsQuery->getValue() . '1');
        $dateTmpl     = new QT(predicate: 'https://date.prop');

        $term                         = new SearchTerm('https://lorem.ipsum', 'ipsum', '@@');
        $config                       = new SearchConfig();
        $config->readFtsConfigFromTerms([$term]);
        $config->ftsStartSel          = '#';
        $config->ftsStopSel           = '#';
        $config->ftsMinWords          = 2;
        $config->ftsMaxWords          = 3;
        $config->ftsMaxFragments      = 10;
        $config->ftsFragmentDelimiter = '|';

        $result        = self::$repo->getResourcesBySearchTerms([$term], $config);
        $meta          = array_map(fn($x) => $x->getMetadata(), iterator_to_array($result));
        $this->assertEquals(2, count($meta));
        $ftsHighlight1 = $meta[0]->getObjectValue($ftsValueTmpl);
        $ftsHighlight2 = $meta[1]->getObjectValue($ftsValueTmpl);
        $date1         = $meta[0]->getObjectValue($dateTmpl);
        $date2         = $meta[1]->getObjectValue($dateTmpl);
        $expected      = [
            '2019-01-01' => 'Lorem #ipsum# dolor',
            '2019-02-01' => 'Lorem #ipsum# dolor|eleifend #ipsum#',
        ];
        $this->assertEquals($expected[$date1], $ftsHighlight1);
        $this->assertEquals($expected[$date2], $ftsHighlight2);
        $this->assertEquals('https://lorem.ipsum', $meta[0]->getObjectValue($ftsPropTmpl));
        $this->assertEquals('https://lorem.ipsum', $meta[1]->getObjectValue($ftsPropTmpl));
        $this->assertEquals('ipsum', $meta[0]->getObjectValue($ftsQueryTmpl));
        $this->assertEquals('ipsum', $meta[1]->getObjectValue($ftsQueryTmpl));
    }

    /**
     * @group search
     */
    public function testSearchFtsHighlight2(): void {
        $ftsValue1Tmpl = new QT(predicate: self::$schema->searchFts->getValue() . '1');
        $ftsValue2Tmpl = new QT(predicate: self::$schema->searchFts->getValue() . '2');
        $ftsQuery1Tmpl = new QT(predicate: self::$schema->searchFtsQuery->getValue() . '1');
        $ftsQuery2Tmpl = new QT(predicate: self::$schema->searchFtsQuery->getValue() . '2');
        $dateTmpl      = new QT(predicate: 'https://date.prop');

        $terms                        = [new SearchTerm('https://lorem.ipsum', 'ipsum', '@@')];
        $config                       = new SearchConfig();
        $config->ftsQuery             = ['ipsum', 'dolor'];
        $config->ftsStartSel          = ['#', '@'];
        $config->ftsStopSel           = ['%', '^'];
        $config->ftsMinWords          = [1, 1];
        $config->ftsMaxWords          = [2, 3];
        $config->ftsMaxFragments      = [2, 2];
        $config->ftsFragmentDelimiter = ['|', '~'];

        $result        = self::$repo->getResourcesBySearchTerms($terms, $config);
        $meta          = array_map(fn($x) => $x->getMetadata(), iterator_to_array($result));
        $this->assertEquals(2, count($meta));
        $date1         = $meta[0]->getObjectValue($dateTmpl);
        $date2         = $meta[1]->getObjectValue($dateTmpl);
        $ftsHighlight1 = [
            $meta[0]->getObjectValue($ftsValue1Tmpl),
            $meta[0]->getObjectValue($ftsValue2Tmpl),
        ];
        $ftsHighlight2 = [
            $meta[1]->getObjectValue($ftsValue1Tmpl),
            $meta[1]->getObjectValue($ftsValue2Tmpl),
        ];
        $ftsQuery1     = [
            $meta[0]->getObjectValue($ftsQuery1Tmpl),
            $meta[0]->getObjectValue($ftsQuery2Tmpl),
        ];
        $ftsQuery2     = [
            $meta[1]->getObjectValue($ftsQuery1Tmpl),
            $meta[1]->getObjectValue($ftsQuery2Tmpl),
        ];
        $order1        = $ftsQuery1[0] === 'ipsum' ? [0, 1] : [1, 0];
        $order2        = $ftsQuery2[0] === 'ipsum' ? [0, 1] : [1, 0];
        $expected      = [
            '2019-01-01' => ['#ipsum% dolor', 'ipsum @dolor^~@dolor^ non neque'],
            '2019-02-01' => ['#ipsum% dolor|#ipsum%', 'ipsum @dolor^~@dolor^ non neque'],
        ];
        for ($i = 0; $i < 2; $i++) {
            $this->assertEquals($config->ftsQuery[$order1[$i]], $ftsQuery1[$i]);
            $this->assertEquals($config->ftsQuery[$order2[$i]], $ftsQuery2[$i]);
            $this->assertEquals($expected[$date1][$order1[$i]], $ftsHighlight1[$i]);
            $this->assertEquals($expected[$date2][$order2[$i]], $ftsHighlight2[$i]);
        }
    }

    /**
     * @group search
     */
    public function testSearchRelatives(): void {
        $dateTmpl = new QT(predicate: 'https://date.prop');

        $query                          = "SELECT id FROM metadata WHERE property = ? AND value = ?";
        $param                          = ['https://date.prop', '2019-02-01'];
        $config                         = new SearchConfig();
        $config->metadataMode           = RepoResource::META_RELATIVES;
        $config->metadataParentProperty = self::$schema->parent;

        $result = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta   = $result[0]->getGraph();
        $this->assertEquals('2019-02-01', $meta->getObjectValue($dateTmpl));
        $parent = $meta->getObject(new QT(predicate: self::$schema->parent));
        $this->assertEquals('2019-01-01', $meta->getDataset()->getObjectValue(new QT($parent, $dateTmpl->getPredicate())));
    }

    /**
     * @group search
     */
    public function testSearchReverseOnly(): void {
        $dateTmpl = new QT(predicate: 'https://date.prop');

        $query                          = "SELECT id FROM identifiers WHERE ids = ?";
        $param                          = ['https://an.unique.id'];
        $config                         = new SearchConfig();
        $config->metadataMode           = '0_0_0_-1';
        $config->metadataParentProperty = self::$schema->parent;

        $result = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta   = $result[0]->getGraph();
        $parent = iterator_to_array($meta->getDataset()->listSubjects(new PT(predicate: self::$schema->parent, object: $meta->getNode())));
        $this->assertCount(1, $parent);
        $rev    = $meta->getDataset()->copy(new QT($parent[0]));
        $this->assertCount(1, $rev);
        $this->assertTrue($rev->every(new QT($parent[0], self::$schema->parent, $meta->getNode())));
    }

    /**
     * @group search
     */
    public function testSearchPaging(): void {
        $dateTmpl = new QT(predicate: 'https://date.prop');

        $query         = "SELECT id FROM metadata WHERE property = ? ORDER BY id";
        $param         = ['https://date.prop'];
        $config        = new SearchConfig();
        $config->limit = 1;

        $config->offset = 0;
        $result         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta           = $result[0]->getGraph();
        $this->assertEquals('2019-01-01', $meta->getObjectValue($dateTmpl));
        $this->assertEquals(2, $config->count);

        $config->offset = 1;
        $config->count  = null;
        $result         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta           = $result[0]->getGraph();
        $this->assertEquals('2019-02-01', $meta->getObjectValue($dateTmpl));
        $this->assertEquals(2, $config->count);
    }

    /**
     * @group search
     */
    public function testSearchOrder(): void {
        $dateProp = 'https://date.prop';
        $dateTmpl = new QT(predicate: $dateProp);
        $query    = "SELECT id FROM metadata WHERE property = ? ORDER BY id";
        $param    = [$dateProp];
        $config   = new SearchConfig();

        $config->orderBy = [$dateProp];
        $results         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $first           = $results[0]->getGraph()->getObjectValue($dateTmpl);
        $second          = $results[1]->getGraph()->getObjectValue($dateTmpl);
        $this->assertGreaterThan($first, $second);

        $config->orderBy = ["^$dateProp"];
        $results         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $first           = $results[0]->getGraph()->getObjectValue($dateTmpl);
        $second          = $results[1]->getGraph()->getObjectValue($dateTmpl);
        $this->assertLessThan($first, $second);
    }

    /**
     * @group search
     */
    public function testSearchOr(): void {
        $terms  = [new SearchTerm(['https://date.prop', 'https://number.prop'], 20)];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', $result[0]->getMetadata()->getObjectValue(new QT(predicate: self::$schema->label)));
    }

    /**
     * Tests if a value can be any identifier of the target resource
     * 
     * @group search
     */
    public function testSearchRelation(): void {
        $terms  = [new SearchTerm(self::$schema->parent, 'https://an.unique.id')];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', $result[0]->getMetadata()->getObjectValue(new QT(predicate: self::$schema->label)));
    }

    /**
     * 
     * @group search
     */
    public function testSearchInverseRelation(): void {
        $terms  = [new SearchTerm(
                SearchTerm::PROPERTY_NEGATE . self::$schema->parent,
                'https://res2.id'
            )];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('sample label for the first resource', $result[0]->getMetadata()->getObjectValue(new QT(predicate: self::$schema->label)));
    }

    /**
     * 
     * @group search
     */
    public function testTooManyRequests(): void {
        $conn = $this->saturateDbConnections();
        try {
            $terms = [new SearchTerm('https://foo/bar')];
            self::$repo->getResourcesBySearchTerms($terms, new SearchConfig());
            /** @phpstan-ignore method.impossibleType */
            $this->assertTrue(false);
        } catch (TooManyRequests $e) {
            /** @phpstan-ignore method.alreadyNarrowedType */
            $this->assertTrue(true);
        }
    }
}
