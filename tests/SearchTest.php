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

use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\SearchTerm;
use zozlak\RdfConstants as C;

/**
 * Description of SearchTest
 *
 * @author zozlak
 */
class SearchTest extends TestBase {

    public function setUp(): void {
        parent::setUp();

        $relProp = self::$repo->getSchema()->parent;
        self::$repo->begin();

        $meta1 = $this->getMetadata([
            self::$schema->id     => 'https://an.unique.id',
            self::$schema->label  => 'sample label for the first resource',
            'https://number.prop' => 150,
            'https://lorem.ipsum' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed iaculis nisl enim, malesuada tempus nisl ultrices ut. Duis egestas at arcu in blandit. Nulla eget sem urna. Sed hendrerit enim ut ultrices luctus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est.',
        ]);
        $meta1->add('https://date.prop', new \EasyRdf\Literal('2019-01-01', null, C::XSD_DATE));
        $res1  = self::$repo->createResource($meta1);
        $this->noteResource($res1);

        $meta2 = $this->getMetadata([
            self::$schema->id     => 'https://res2.id',
            $relProp              => $res1->getUri(),
            self::$schema->label  => 'a more original title for a resource',
            'https://number.prop' => 20,
            'https://lorem.ipsum' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Curabitur non dolor non neque venenatis aliquet vitae venenatis est. Aenean eleifend ipsum eu placerat sagittis. Aenean ullamcorper dignissim enim, ut congue turpis tristique eu.',
        ]);
        $meta2->add('https://date.prop', new \EasyRdf\Literal('2019-02-01', null, C::XSD_DATE));
        $res2  = self::$repo->createResource($meta2);
        $this->noteResource($res2);

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
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getLiteral(self::$schema->label));
    }

    /**
     * @group search
     */
    public function testSearchBySqlQueryEmpty(): void {
        $result = self::$repo->getResourcesBySqlQuery("SELECT -1 AS id WHERE false", [
            ], new SearchConfig());
        $this->assertEquals([], iterator_to_array($result));
    }

    /**
     * @group search
     */
    public function testSearchBySearchTerms(): void {
        $term1  = new SearchTerm('https://number.prop', 30, '<=', C::XSD_DECIMAL);
        $term2  = new SearchTerm('https://date.prop', '2019-01-15', '>=', C::XSD_DATE);
        $term3  = new SearchTerm('https://lorem.ipsum', 'ipsum', '@@');
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms([$term1,
                $term2, $term3], new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getLiteral(self::$schema->label));
    }

    /**
     * @group search
     */
    public function testSearchWrongDataType(): void {
        $term1  = new SearchTerm('https://number.prop', 30, '<=', C::XSD_STRING);
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms([$term1], new SearchConfig()));
        $this->assertEquals(2, count($result));
    }

    /**
     * @group search
     */
    public function testSearchFtsHighlight(): void {
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
        $result        = iterator_to_array($result);
        $this->assertEquals(2, count($result));
        $ftsValueProp  = self::$repo->getSchema()->searchFts;
        $ftsPropProp   = self::$repo->getSchema()->searchFtsProperty;
        $ftsQueryProp  = self::$repo->getSchema()->searchFtsQuery;
        $ftsHighlight1 = (string) $result[0]->getMetadata()->getLiteral($ftsValueProp . '1');
        $ftsHighlight2 = (string) $result[1]->getMetadata()->getLiteral($ftsValueProp . '1');
        $date1         = (string) $result[0]->getMetadata()->getLiteral('https://date.prop');
        $date2         = (string) $result[1]->getMetadata()->getLiteral('https://date.prop');
        $expected      = [
            '2019-01-01' => 'Lorem #ipsum# dolor',
            '2019-02-01' => 'Lorem #ipsum# dolor|eleifend #ipsum#',
        ];
        $this->assertEquals($expected[$date1], $ftsHighlight1);
        $this->assertEquals($expected[$date2], $ftsHighlight2);
        $this->assertEquals('https://lorem.ipsum', (string) $result[0]->getMetadata()->get($ftsPropProp . '1'));
        $this->assertEquals('https://lorem.ipsum', (string) $result[1]->getMetadata()->get($ftsPropProp . '1'));
        $this->assertEquals('ipsum', (string) $result[0]->getMetadata()->getLiteral($ftsQueryProp . '1'));
        $this->assertEquals('ipsum', (string) $result[1]->getMetadata()->getLiteral($ftsQueryProp . '1'));
    }

    /**
     * @group search
     */
    public function testSearchFtsHighlight2(): void {
        $term                         = new SearchTerm('https://lorem.ipsum', 'ipsum', '@@');
        $config                       = new SearchConfig();
        $config->ftsQuery             = ['ipsum', 'dolor'];
        $config->ftsStartSel          = ['#', '@'];
        $config->ftsStopSel           = ['%', '^'];
        $config->ftsMinWords          = [1, 1];
        $config->ftsMaxWords          = [2, 3];
        $config->ftsMaxFragments      = [2, 2];
        $config->ftsFragmentDelimiter = ['|', '~'];

        $result        = iterator_to_array(self::$repo->getResourcesBySearchTerms([
                $term], $config));
        $this->assertEquals(2, count($result));
        $ftsValueProp  = self::$repo->getSchema()->searchFts;
        $ftsQueryProp  = self::$repo->getSchema()->searchFtsQuery;
        $date1         = (string) $result[0]->getMetadata()->getLiteral('https://date.prop');
        $date2         = (string) $result[1]->getMetadata()->getLiteral('https://date.prop');
        $ftsHighlight1 = [
            (string) $result[0]->getMetadata()->getLiteral($ftsValueProp . '1'),
            (string) $result[0]->getMetadata()->getLiteral($ftsValueProp . '2'),
        ];
        $ftsHighlight2 = [
            (string) $result[1]->getMetadata()->getLiteral($ftsValueProp . '1'),
            (string) $result[1]->getMetadata()->getLiteral($ftsValueProp . '2'),
        ];
        $ftsQuery1     = [
            (string) $result[0]->getMetadata()->getLiteral($ftsQueryProp . '1'),
            (string) $result[0]->getMetadata()->getLiteral($ftsQueryProp . '2'),
        ];
        $ftsQuery2     = [
            (string) $result[1]->getMetadata()->getLiteral($ftsQueryProp . '1'),
            (string) $result[1]->getMetadata()->getLiteral($ftsQueryProp . '2'),
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
        $query                          = "SELECT id FROM metadata WHERE property = ? AND value = ?";
        $param                          = ['https://date.prop', '2019-02-01'];
        $config                         = new SearchConfig();
        $config->metadataMode           = RepoResource::META_RELATIVES;
        $config->metadataParentProperty = self::$repo->getSchema()->parent;

        $result     = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta       = $result[0]->getGraph();
        $this->assertEquals('2019-02-01', (string) $meta->getLiteral('https://date.prop'));
        $parentProp = self::$repo->getSchema()->parent;
        $this->assertEquals('2019-01-01', (string) $meta->getResource($parentProp)?->getLiteral('https://date.prop'));
    }

    /**
     * @group search
     */
    public function testSearchPaging(): void {
        $query         = "SELECT id FROM metadata WHERE property = ? ORDER BY id";
        $param         = ['https://date.prop'];
        $config        = new SearchConfig();
        $config->limit = 1;

        $config->offset = 0;
        $result         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta           = $result[0]->getGraph();
        $this->assertEquals('2019-01-01', (string) $meta->getLiteral('https://date.prop'));
        $this->assertEquals(2, $config->count);

        $config->offset = 1;
        $config->count  = null;
        $result         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $this->assertEquals(1, count($result));
        $meta           = $result[0]->getGraph();
        $this->assertEquals('2019-02-01', (string) $meta->getLiteral('https://date.prop'));
        $this->assertEquals(2, $config->count);
    }

    /**
     * @group search
     */
    public function testSearchOrder(): void {
        $dateProp = 'https://date.prop';
        $query    = "SELECT id FROM metadata WHERE property = ? ORDER BY id";
        $param    = [$dateProp];
        $config   = new SearchConfig();

        $config->orderBy = [$dateProp];
        $results         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $first           = (string) $results[0]->getGraph()->get($dateProp);
        $second          = (string) $results[1]->getGraph()->get($dateProp);
        $this->assertGreaterThan($first, $second);

        $config->orderBy = ["^$dateProp"];
        $results         = iterator_to_array(self::$repo->getResourcesBySqlQuery($query, $param, $config));
        $first           = (string) $results[0]->getGraph()->get($dateProp);
        $second          = (string) $results[1]->getGraph()->get($dateProp);
        $this->assertLessThan($first, $second);
    }

    /**
     * @group search
     */
    public function testSearchOr(): void {
        $terms  = [new SearchTerm(['https://date.prop', 'https://number.prop'], 20)];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getLiteral(self::$schema->label));
    }

    /**
     * Tests if a value can be any identifier of the target resource
     * 
     * @group search
     */
    public function testSearchRelation(): void {
        $terms  = [new SearchTerm(self::$repo->getSchema()->parent, 'https://an.unique.id')];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('a more original title for a resource', (string) $result[0]->getMetadata()->getLiteral(self::$schema->label));
    }

    /**
     * 
     * @group search
     */
    public function testSearchInverseRelation(): void {
        $terms  = [new SearchTerm(
                SearchTerm::PROPERTY_NEGATE . self::$repo->getSchema()->parent,
                'https://res2.id'
        )];
        $result = iterator_to_array(self::$repo->getResourcesBySearchTerms($terms, new SearchConfig()));
        $this->assertEquals(1, count($result));
        $this->assertEquals('sample label for the first resource', (string) $result[0]->getMetadata()->getLiteral(self::$schema->label));
    }
}
