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

/**
 * Simple container for an SQL query and its parameters
 *
 * @author zozlak
 */
class QueryPart {

    static private $n = 1;

    /**
     *
     * @var string
     */
    public $query;

    /**
     *
     * @var array
     */
    public $param;

    /**
     * 
     * @param string $query
     * @param array $param
     */
    public function __construct(string $query = '', array $param = []) {
        $this->query = $query;
        $this->param = $param;
    }

    /**
     * Pastes the join code and the query if the query is not empty.
     * 
     * If the query is empty, returns an empty string.
     * 
     * @param string $type left side of the join clause, e.g. `LEFT JOIN`
     * @param string $clause right side of the join clause, e.g. `USING(id)`
     * @return string
     */
    public function join(string $type, string $clause): string {
        if (empty($this->query)) {
            return '';
        }
        self::$n++;
        return $type . " (" . $this->query . ") _t" . self::$n . " " . $clause;
    }

}
