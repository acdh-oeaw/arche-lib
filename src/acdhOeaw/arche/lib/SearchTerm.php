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

use zozlak\RdfConstants as RDF;
use zozlak\queryPart\QueryPart;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Describes a single search condition.
 * 
 * Provides mappings to SQL queries and REST request parameters.
 *
 * @author zozlak
 */
class SearchTerm {

    const PROPERTY_BINARY   = 'BINARY';
    const PROPERTY_NEGATE   = '^';
    const DATETIME_REGEX    = '/^-?[0-9]{4,}-[0-9][0-9]-[0-9][0-9](T[0-9][0-9](:[0-9][0-9])?(:[0-9][0-9])?([.][0-9]+)?Z?)?$/';
    const URI_REGEX         = '^\w+:(\/?\/?)[^\s]+';
    const TYPE_NUMBER       = 'number';
    const TYPE_DATE         = 'date';
    const TYPE_DATETIME     = 'datetime';
    const TYPE_STRING       = 'string';
    const TYPE_RELATION     = 'relation';
    const TYPE_FTS          = 'fts';
    const TYPE_SPATIAL      = 'spatial';
    const TYPE_ID           = 'id';
    const OPERATOR_IN       = 'in';
    const OPERATOR_FTS      = '@@';
    const COLUMN_STRING     = 'value';
    const STRING_MAX_LENGTH = 1000;

    /**
     * List of operators and data types they enforce
     * @var array<string, string|null>
     */
    static private array $operators = [
        '='                => null,
        '>'                => null,
        '<'                => null,
        '<='               => null,
        '>='               => null,
        '~'                => self::TYPE_STRING,
        '~*'               => self::TYPE_STRING,
        self::OPERATOR_FTS => self::TYPE_FTS,
        '&&'               => self::TYPE_SPATIAL,
        '&&&'              => self::TYPE_SPATIAL,
        '&>'               => self::TYPE_SPATIAL,
        '&<'               => self::TYPE_SPATIAL,
    ];

    /**
     * 
     * @var array<string, string>
     */
    static private array $typesToColumns = [
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
        RDF::XSD_ANY_URI       => 'value',
        RDF::RDFS_RESOURCE     => 'ids',
        self::TYPE_DATE        => 'value_t::date',
        self::TYPE_DATETIME    => 'value_t',
        self::TYPE_NUMBER      => 'value_n',
        self::TYPE_STRING      => 'value',
        self::TYPE_RELATION    => 'ids',
        self::TYPE_ID          => 'id',
    ];

    static public function escapeFts(string $value): string {
        // escape URIs/URLs so that websearch_to_tsquery() parses them properly
        $value = (string) preg_replace("`" . self::URI_REGEX . "`", '"\0"', $value);
        return str_replace('""', '"', $value);
    }

    /**
     * Creates an instance of the SearchTerm class from a given $_POST vars set
     * 
     * @param int $key
     * @return SearchTerm
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
     * @var array<string> | string
     */
    public null | array | string $property;

    /**
     * Operator to be used for the RDF triple value comparison.
     * 
     * One of `=`, `<`, `<=`, `>`, `>=`, `~` (regular expresion match), 
     * `@@` (full text search match), 
     * 
     * @see SearchTerm::$value
     */
    public ?string $operator;

    /**
     * Value to be matched by the RDF triple (with a given operator)
     * 
     * @var array<scalar> | string | int | float | bool
     * @see SearchTerm::$operator
     */
    public null | array | string | int | float | bool $value;

    /**
     * Data type to be matched by the RDF triple.
     * 
     * Should be one of main XSD data types or one of `TYPE_...` constants defined by this class.
     */
    public ?string $type;

    /**
     * Language to be matched by the RDF triple
     */
    public ?string $language;

    /**
     * Creates a search term object.
     * 
     * @param array<string>|string|null $property property to be matched by the RDF triple
     *   if prefixed with a `self::PROPERTY_NEGATE`, the rdfs:resource $type is enforced 
     *   and the $value is evaluated against the triple subject (instead of object)
     * @param array<scalar>|scalar|null $value value to be matched by the RDF triple (with a given operator)
     * @param string $operator operator used to compare the RDF triple value
     * @param string|null $type value to be matched by the RDF triple 
     *   (one of base XSD types or one of `TYPE_...` constants defined by this class)
     * @param string|null $language language to be matched by the RDF triple
     */
    public function __construct($property = null, $value = null,
                                string $operator = '=', ?string $type = null,
                                ?string $language = null) {
        $this->property = $property;
        $this->operator = $operator;
        $this->type     = $type;
        $this->value    = $value;
        $this->language = $language;

        if (!in_array(substr($this->operator, 0, 2), array_keys(self::$operators))) {
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
     * @param array<string> $nonRelationProperties list of properties which are internally
     *   stored only as literals, even if they are resources in the RDF graph
     * @return QueryPart
     */
    public function getSqlQuery(string $baseUrl, string $idProp,
                                array $nonRelationProperties): QueryPart {
        if (is_array($this->property) || is_array($this->value)) {
            return $this->getSqlQueryOr($baseUrl, $idProp, $nonRelationProperties);
        }

        if (str_starts_with($this->property ?? '', self::PROPERTY_NEGATE)) {
            $type = self::TYPE_RELATION;
        } else {
            $type = self::$operators[substr($this->operator ?? '', 0, 2)]; // substr for spatial operators with distance
        }
        // if type not enforced by the property direction nor operator, try the provided one
        $type = $type ?? $this->type;
        // if type still unknown guess it from the value
        if ($type === null) {
            if (is_numeric($this->value)) {
                $type = self::TYPE_NUMBER;
            } elseif (preg_match(self::DATETIME_REGEX, (string) $this->value)) {
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
            case self::TYPE_ID:
                return $this->getSqlQueryId();
            case self::TYPE_FTS:
                return $this->getSqlQueryFts($idProp);
            case self::TYPE_SPATIAL:
                return $this->getSqlQuerySpatial();
            case self::TYPE_RELATION:
            case RDF::RDFS_RESOURCE:
                return $this->getSqlQueryUri($this->property ?? '');
            default:
                return $this->getSqlQueryMeta($type, $baseUrl, $idProp);
        }
    }

    /**
     * 
     * @param string $baseUrl
     * @param string $idProp
     * @param array<string> $nonRelationProperties
     * @return QueryPart
     */
    private function getSqlQueryOr(string $baseUrl, string $idProp,
                                   array $nonRelationProperties): QueryPart {
        $properties = is_array($this->property) ? $this->property : [$this->property];
        $values     = is_array($this->value) ? $this->value : [$this->value];
        $query      = new QueryPart();
        $n          = 0;
        foreach ($properties as $property) {
            foreach ($values as $value) {
                $term           = clone $this;
                $term->property = $property;
                $term->value    = $value;
                $termQuery      = $term->getSqlQuery($baseUrl, $idProp, $nonRelationProperties);
                $query->query   .= ($n > 0 ? " UNION " : '') . $termQuery->query . "\n";
                $query->param   = array_merge($query->param, $termQuery->param);
                $n++;
            }
        }
        return $query;
    }

    private function getSqlQueryFts(string $idProp): QueryPart {
        $value = is_array($this->value) ? reset($this->value) : $this->value;
        $value = self::escapeFts((string) $value);
        $param = [$value];
        $where = '';
        if (!empty($this->language)) {
            $where   .= " AND (lang = ? OR lang IS NULL)";
            $param[] = $this->language;
        }
        if (!empty($this->property)) {
            if ($this->property === self::PROPERTY_BINARY) {
                $where .= " AND mid IS NULL AND iid IS NULL";
            } elseif ($this->property === $idProp) {
                $where .= " AND iid IS NOT NULL";
            } else {
                $where   .= " AND property = ?";
                $param[] = $this->property;
            }
        }
        $query = "
            SELECT COALESCE(m.id, fts.iid, fts.id) AS id
            FROM full_text_search fts LEFT JOIN metadata m USING (mid)
            WHERE websearch_to_tsquery('simple', ?) @@ segments $where
        ";
        return new QueryPart($query, $param);
    }

    private function getSqlQueryUri(string $property): QueryPart {
        $value  = $this->value;
        $select = "r.id";
        $on     = "r.target_id = i.id";
        if (str_starts_with($property, self::PROPERTY_NEGATE)) {
            $property = substr($property, strlen(self::PROPERTY_NEGATE));
            $id       = $this->value;
            $select   = "r.target_id AS id";
            $on       = "r.id = i.id";
        }

        $where = $param = [];
        if (!empty($property)) {
            $where[] = "property = ?";
            $param[] = $property;
        }
        if (!empty($value)) {
            $where[] = "ids = ?";
            $param[] = $value;
        }
        if (count($where) === 0) {
            throw new RepoLibException('Empty search term', 400);
        }
        $where = implode(' AND ', $where);
        $query = "
            SELECT $select
            FROM relations r JOIN identifiers i ON $on
            WHERE $where
        ";
        return new QueryPart($query, $param);
    }

    private function getSqlQuerySpatial(): QueryPart {
        $param      = [$this->value];
        $valueQuery = 'st_geomfromtext(?, 4326)';
        switch ($this->operator ?? '') {
            case '&>':
                $func = "st_contains(geom::geometry, $valueQuery)";
                break;
            case '&<':
                $func = "st_contains($valueQuery, geom::geometry)";
                break;
            case '&&&':
                $func = "st_intersects(geom, $valueQuery)";
                break;
            case '&&':
            default:
                $dist = (int) substr($this->operator ?? '', 2);
                if ($dist > 0) {
                    $func    = "st_dwithin(geom, $valueQuery::geography, ?, false)";
                    $param[] = $dist;
                } else {
                    $func = "geom && $valueQuery";
                }
                break;
        }
        $query = "
            SELECT COALESCE(m.id, ss.id) AS id
            FROM spatial_search ss LEFT JOIN metadata m USING (mid)
            WHERE $func
        ";
        return new QueryPart($query, $param);
    }

    private function getSqlQueryMeta(string $type, string $baseUrl,
                                     string $idProp): QueryPart {
        $column      = self::$typesToColumns[$type];
        $columnRaw   = $column;
        $otherTables = true;

        $where = $param = [];
        if (!empty($this->property)) {
            $where[] = 'property = ?';
            $param[] = $this->property;
        }
        if (!empty($this->language)) {
            $where[]     = 'lang = ?';
            $param[]     = $this->language;
            $otherTables = false;
        }
        if (!empty($this->value) && is_scalar($this->value)) {
            $otherTables = $otherTables && $column === self::COLUMN_STRING;
            // string values stored in the database can be too long to be indexed, 
            // therefore the index is set only on `substring(value, 1, self::STRING_MAX_LENGTH)`
            // and to benefit from it the predicate must strictly follow the index definition
            if ($column === self::COLUMN_STRING && $this->operator === '=' && strlen((string) $this->value) < self::STRING_MAX_LENGTH) {
                $column = "substring(" . $column . ", 1, " . self::STRING_MAX_LENGTH . ")";
            }
            if (substr($column, 0, 7) !== 'value_t') {
                $where[] = $column . ' ' . $this->operator . ' ?';
                $param[] = $this->value;
            } else {
                // dates require special handling taking into account they might be out of the timestamp range
                $valueN = (int) $this->value;
                if ($valueN >= -4713) {
                    $valueT = (string) $this->value;
                    if (substr($valueT, 0, 1) === '-') {
                        $valueT = substr($valueT, 1) . ' BC';
                    }
                    $where[] = '(value_t IS NOT NULL AND value_t ' . $this->operator . ' ? OR value_t IS NULL AND value_n ' . $this->operator . ' ?)';
                    $param[] = $valueT;
                    $param[] = $valueN;
                } else {
                    $where[] = 'value_n ' . $this->operator . ' ?';
                    $param[] = $valueN;
                }
            }
        }
        if (count($where) === 0) {
            throw new RepoLibException('Empty search term', 400);
        }
        $where = implode(' AND ', $where);
        $query = "
            SELECT id
            FROM metadata
            WHERE $where
        ";
        if ($otherTables) {
            $where = str_replace($column, $columnRaw, $where);
            $query .= "
              UNION
                SELECT id
                FROM (SELECT id, ? AS property, '' AS lang, ids AS value FROM identifiers) t
                WHERE $where
              UNION
                SELECT id
                FROM (SELECT r.id, property, '' AS lang, ids AS value FROM relations r JOIN identifiers i ON r.target_id = i.id) t
                WHERE $where
            ";
            $param = array_merge($param, [$idProp], $param, $param);
        }
        return new QueryPart($query, $param);
    }

    private function getSqlQueryId(): QueryPart {
        if (is_array($this->value)) {
            $query = 'SELECT id FROM (VALUES ' . substr(str_repeat(', (?::bigint)', count($this->value)), 2) . ') AS t (id)';
            return new QueryPart($query, $this->value);
        } else {
            return new QueryPart('SELECT ?::bigint AS id', [$this->value]);
        }
    }

    /**
     * Returns the search term formatted as an HTTP query string.
     * 
     * @param int $n term number. It's required for propert serialization of
     *   multi-property and multi-values search terms.
     * @return string
     */
    public function getFormData(int $n = 0): string {
        $terms = [];
        foreach ((array) $this as $k => $v) {
            if ($v !== null) {
                $terms[$k . "[$n]"] = is_array($v) ? $v : (string) $v;
            }
        }
        return http_build_query($terms);
    }
}
