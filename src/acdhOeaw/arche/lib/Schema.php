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
 * A container for configuration properties.
 * 
 * Assures deep copies of complex configuration objects being returned to prevent
 * surprising configuration modifications.
 *
 * @author zozlak
 * @property NamedNode $id
 * @property NamedNode $label
 * @property NamedNode $parent
 * @property NamedNode $delete
 * @property NamedNode $binarySize
 * @property NamedNode $hash
 * @property NamedNode $mime
 * @property NamedNode $fileName
 * @property NamedNode $searchCount
 * @property float $searchWeight
 * @property NamedNode $searchFts
 * @property NamedNode $searchFtsQuery
 * @property NamedNode $searchFtsProperty
 * @property NamedNode $searchMatch
 * @property NamedNode $searchOrder
 * @property NamedNode $searchOrderValue
 * @property NamedNode $creationDate
 * @property NamedNode $creationUser
 * @property NamedNode $modificationDate
 * @property NamedNode $modificationUser
 * @property NamedNode $binaryModificationDate
 * @property NamedNode $binaryModificationUser
 * @property Schema $test
 */
class Schema {

    private object $schema;

    /**
     * Creates the Schema object.
     * 
     * @param object $schema object with configuration properties
     */
    public function __construct(object $schema) {
        foreach (get_object_vars($schema) as $k => $v) {
            if ($v instanceof \stdClass) {
                $schema->$k = new Schema($v);
            } else {
                $schema->$k = DF::namedNode($v);
            }
        }
        unset($v);
        $this->schema = $schema;
    }

    /**
     * Magic method implementing accessing properties.
     * 
     * @param string $name configuration property to be returned
     * @return Schema|NamedNode|null
     */
    public function __get(string $name): Schema | NamedNode | null {
        return $this->schema->$name ?? null;
    }

    public function __set(string $name, mixed $value) {
        throw new \BadMethodCallException();
    }
}
