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

namespace acdhOeaw\arche\lib;

use quickRdf\DataFactory as DF;
use quickRdf\NamedNode;

/**
 * An immutable container for RDF property mappings schema.
 * 
 * @author zozlak
 * @property NamedNode $accessRestriction
 * @property NamedNode $accessRestrictionAgg
 * @property NamedNode $accessRole
 * @property NamedNode $binaryModificationDate
 * @property NamedNode $binaryModificationUser
 * @property NamedNode $binarySize
 * @property NamedNode $binarySizeCumulative
 * @property object    $classes
 * @property NamedNode $cmdi
 * @property NamedNode $cmdiPid
 * @property NamedNode $countCumulative
 * @property NamedNode $creationDate
 * @property NamedNode $creationUser
 * @property NamedNode $dateStart
 * @property NamedNode $dateEnd
 * @property NamedNode $delete
 * @property NamedNode $fileName
 * @property NamedNode $hash
 * @property NamedNode $id
 * @property NamedNode $info
 * @property Schema    $ingest
 * @property NamedNode $isNewVersionOf
 * @property NamedNode $label
 * @property NamedNode $latitude
 * @property NamedNode $license
 * @property NamedNode $licenseAgg
 * @property NamedNode $longitude
 * @property NamedNode $mime
 * @property NamedNode $modificationDate
 * @property NamedNode $modificationUser
 * @property Schema    $namespaces
 * @property NamedNode $ontology
 * @property NamedNode $parent
 * @property NamedNode $pid
 * @property NamedNode $searchCount
 * @property NamedNode $searchFts
 * @property NamedNode $searchFtsQuery
 * @property NamedNode $searchFtsProperty
 * @property NamedNode $searchMatch
 * @property NamedNode $searchOrder
 * @property NamedNode $searchOrderValue
 * @property float     $searchWeight
 * @property Schema    $test
 * @property NamedNode $url
 * @property NamedNode $version
 * @property NamedNode $vid
 * @property NamedNode $wkt
 */
class Schema implements \Iterator {

    private array $schema;

    /**
     * Creates the Schema object.
     * 
     * @param object|array<mixed> $schema object with configuration properties
     */
    public function __construct(object | array $schema) {
        if (is_object($schema)) {
            $schema = get_object_vars($schema);
        }
        foreach ($schema as $k => $v) {
            if (is_object($v) || is_array($v)) {
                $this->schema[$k] = new Schema($v);
            } else {
                $this->schema[$k] = DF::namedNode((string) $v);
            }
        }
    }

    /**
     * Magic method implementing accessing properties.
     * 
     * @param string $name configuration property to be returned
     * @return Schema|NamedNode|null
     */
    public function __get(string $name): Schema | NamedNode | null {
        return $this->schema[$name] ?? null;
    }

    /**
     * 
     * @param string $name
     * @param mixed $value
     * @throws \BadMethodCallException
     */
    public function __set(string $name, mixed $value) {
        throw new \BadMethodCallException();
    }

    public function current(): mixed {
        return current($this->schema);
    }

    public function key(): mixed {
        return key($this->schema);
    }

    public function next(): void {
        next($this->schema);
    }

    public function rewind(): void {
        reset($this->schema);
    }

    public function valid(): bool {
        return current($this->schema) !== false;
    }
}
