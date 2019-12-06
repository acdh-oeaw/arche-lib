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

use zozlak\RdfConstants as RDF;
use acdhOeaw\acdhRepoLib\exception\RepoLibException;

/**
 * Describes a single search condition.
 * 
 * Provides mappings to SQL queries and REST request parameters.
 *
 * @author zozlak
 */
class SearchTerm {

    const DATETIME_REGEX    = '/^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T[0-9][0-9](:[0-9][0-9])?(:[0-9][0-9])?([.][0-9]+)?Z?)?$/';
    const TYPE_NUMBER       = 'number';
    const TYPE_DATE         = 'date';
    const TYPE_DATETIME     = 'datetime';
    const TYPE_STRING       = 'string';
    const TYPE_RELATION     = 'relation';
    const TYPE_FTS          = 'fts';
    const COLUMN_STRING     = 'value';
    const STRING_MAX_LENGTH = 1000;

    /**
     * List of operators and data types they enforce
     * @var array
     */
    static private $operators      = [
        '='  => null,
        '>'  => null,
        '<'  => null,
        '<=' => null,
        '>=' => null,
        '~'  => self::TYPE_STRING,
        '@@' => self::TYPE_FTS,
    ];
    static private $typesToColumns = [
        RDF::XSD_STRING        => 'value',
        RDF::XSD_BOOLEAN       => 'value_n',
        RDF::XSD_DECIMAL       => 'value_n',
        RDF::XSD_FLOAT         => 'value_n',
        RDF::XSD_DOUBLE        => 'value_n',
        RDF::XSD_DURATION      => 'value',
        RDF::XSD_DATE_TIME     => 'value_t',
        RDF::XSD_TIME          => 'value_t::time',
        RDF::XSD_DATE          => 'value_t::date',
        RDF::XSD_HEX_BINARY    => 'value',
        RDF::XSD_BASE64_BINARY => 'value',
        RDF::XSD_ANY_URI       => 'ids',
        self::TYPE_DATE        => 'value_t::date',
        self::TYPE_DATETIME    => 'value_t',
        self::TYPE_NUMBER      => 'value_n',
        self::TYPE_STRING      => 'value',
        self::TYPE_RELATION    => 'ids',
    ];

    /**
     * Creates an instance of the SearchTerm class from a given $_POST vars set
     * 
     * @param type $key
     * @return \acdhOeaw\acdhRepoLib\SearchTerm
     */
    static public function factory($key): self {
        $property = $_POST['property'][$key] ?? null;
        $value    = $_POST['value'][$key] ?? null;
        $operator = $_POST['operator'][$key] ?? '=';
        $type     = $_POST['type'][$key] ?? null;
        $language = $_POST['language'][$key] ?? null;
        return new SearchTerm($property, $value, $operator, $type, $language);
    }

    /**
     * Property to be matched by the RDF triple.
     * 
     * @var string
     */
    public $property;

    /**
     * Operator to be used for the RDF triple value comparison.
     * 
     * One of `=`, `<`, `<=`, `>`, `>=`, `~` (regular expresion match), `@@` (full text search match)
     * 
     * @var string
     * @see $value
     */
    public $operator;

    /**
     * Value to be matched by the RDF triple (with a given operator)
     * 
     * @var mixed
     * @see $operator
     */
    public $value;

    /**
     * Data type to be matched by the RDF triple.
     * 
     * Should be one of main XSD data types or one of `TYPE_...` constants defined by this class.
     * 
     * @var string
     */
    public $type;

    /**
     * Language to be matched by the RDF triple
     * 
     * @var string
     */
    public $language;

    /**
     * Creates a search term object.
     * 
     * @param string|null $property property to be matched by the RDF triple
     * @param type $value value to be matched by the RDF triple (with a given operator)
     * @param string $operator operator used to compare the RDF triple value
     * @param string|null $type value to be matched by the RDF triple 
     *   (one of base XSD types or one of `TYPE_...` constants defined by this class)
     * @param string|null $language language to be matched by the RDF triple
     */
    public function __construct(?string $property = null, $value = null,
                                string $operator = '=', ?string $type = null,
                                ?string $language = null) {
        $this->property = $property;
        $this->operator = $operator;
        $this->type     = $type;
        $this->value    = $value;
        $this->language = $language;

        if (!in_array($this->operator, array_keys(self::$operators))) {
            throw new RepoLibException('Unknown operator ' . $this->operator, 400);
        }
        if (!in_array($this->type, array_keys(self::$typesToColumns)) && $this->type !== null) {
            throw new RepoLibException('Unknown type ' . $this->type, 400);
        }
    }

    /**
     * Returns an SQL query part returning ids of resources matching the search term.
     * 
     * @param string $baseUrl repository base URL
     * @param string $idProp RDF property denoting identifiers
     * @param array $nonRelationProperties list of properties which are internally
     *   stored only as literals, even if they are resources in the RDF graph
     * @return \acdhOeaw\acdhRepoLib\QueryPart
     */
    public function getSqlQuery(string $baseUrl, string $idProp, array $nonRelationProperties): QueryPart {
        $type = self::$operators[$this->operator];
        // if type not enforced by the operator, try the provided one
        if ($type === null) {
            $type = $this->type;
        }
        // if type not enforced by the operator and not provided, guess it
        if ($type === null) {
            if (is_numeric($this->value)) {
                $type = self::TYPE_NUMBER;
            } elseif (preg_match(self::DATETIME_REGEX, $this->value)) {
                $type = self::TYPE_DATETIME;
            } else {
                $type = self::TYPE_STRING;
            }
        }
        // non-relation properties
        if (in_array($this->property, $nonRelationProperties)) {
            $type = self::TYPE_STRING;
        }

        switch ($type) {
            case self::TYPE_FTS:
                return $this->getSqlQueryFts();
            case self::TYPE_RELATION:
            case RDF::XSD_ANY_URI:
                return $this->getSqlQueryUri();
            default:
                return $this->getSqlQueryMeta($type, $baseUrl, $idProp);
        }
    }

    private function getSqlQueryFts(): QueryPart {
        $param = [$this->value];
        $where = '';
        if (!empty($this->property)) {
            $where   .= " AND property = ?";
            $param[] = $this->property;
        }
        $query = "
            SELECT DISTINCT id 
            FROM full_text_search 
            WHERE websearch_to_tsquery('simple', ?) @@ segments $where
        ";
        return new QueryPart($query, $param);
    }

    private function getSqlQueryUri(): QueryPart {
        $where = $param = [];
        if (!empty($this->property)) {
            $where[] = "property = ?";
            $param[] = $this->property;
        }
        if (!empty($this->value)) {
            $where[] = "ids = ?";
            $param[] = $this->value;
        }
        if (count($where) === 0) {
            throw new RepoLibException('Empty search term', 400);
        }
        $where = implode(' AND ', $where);
        $query = "
            SELECT DISTINCT r.id
            FROM relations r JOIN identifiers i ON r.target_id = i.id
            WHERE $where
        ";
        return new QueryPart($query, $param);
    }

    private function getSqlQueryMeta(string $type, string $baseUrl, string $idProp): QueryPart {
        $where = $param = [];
        if (!empty($this->property)) {
            $where[] = 'property = ?';
            $param[] = $this->property;
        }
        if (!empty($this->language)) {
            $where[] = 'lang = ?';
            $param[] = $this->language;
        }
        $otherTables = false;
        if (!empty($this->value)) {
            $column      = self::$typesToColumns[$type];
            $otherTables = $column === self::COLUMN_STRING;
            // string values stored in the database can be to long to be indexed, 
            // therefore the index is set only on `substring(value, 1, self::STRING_MAX_LENGTH)`
            // and to benefit from it the predicate must strictly follow the index definition
            if ($column === self::COLUMN_STRING && strlen($this->value) < self::STRING_MAX_LENGTH) {
                $column = "substring(" . $column . ", 1, " . self::STRING_MAX_LENGTH . ")";
            }

            $where[] = $column . ' ' . $this->operator . ' ?';
            $param[] = $this->value;
        }
        if (count($where) === 0) {
            throw new RepoLibException('Empty search term', 400);
        }
        $where = implode(' AND ', $where);
        $query = "
            SELECT DISTINCT id
            FROM metadata
            WHERE $where
        ";
        if ($otherTables) {
            $query   .= "
              UNION
                SELECT DISTINCT id
                FROM (SELECT id, ? AS property, '' AS lang, ids AS value FROM identifiers) t
                WHERE $where
              UNION
                SELECT DISTINCT id
                FROM (SELECT id, property, '' AS lang, ? || target_id AS value FROM relations) t
                WHERE $where
            ";
            $param   = array_merge($param, [$idProp], $param, [$baseUrl], $param);
        }
        return new QueryPart($query, $param);
    }

    /**
     * Returns the search term formatted as an HTTP query string.
     * 
     * @return string
     */
    public function getFormData(): string {
        $terms = [];
        foreach ($this as $k => $v) {
            if ($v !== null) {
                $terms[$k . '[]'] = (string) $v;
            }
        }
        return http_build_query($terms);
    }

}
