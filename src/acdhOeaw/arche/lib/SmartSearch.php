<?php

/*
 * The MIT License
 *
 * Copyright 2023 Austrian Centre for Digital Humanities.
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

use Generator;
use PDO;
use PDOStatement;
use Psr\Log\AbstractLogger;
use zozlak\queryPart\QueryPart;
use zozlak\RdfConstants as RDF;

/**
 * Provides an API for advanced weighted 
 *
 * @author zozlak
 */
class SmartSearch {

    const TEMPTABNAME = "_matches";

    private PDO $pdo;
    private RepoDb $repo;
    private Schema $schema;

    /**
     * 
     * @var array<string, float>
     */
    private array $propWeights       = [];
    private float $propDefaultWeight = 1.0;

    /**
     * 
     * @var array<object>
     */
    private array $facets = [];

    /**
     * 
     * @var array<string, object>
     */
    private array $rangeFacets           = [];
    private float $facetsDefaultWeight   = 1.0;
    private float $exactWeight           = 10.0;
    private float $langWeight            = 10.0;
    private string $namedEntitiesProperty = RDF::RDF_TYPE;

    /**
     * 
     * @var array<string>
     */
    private array $namedEntitiesValues = [];

    /**
     * 
     * @var array<string, float>
     */
    private array $namedEntityWeights       = [];
    private float $namedEntityDefaultWeight = 1.0;
    private string $phrase;
    private string $orderExp                 = '';
    private string $dateFacetsFormat         = 'YYYY-MM-DD';
    private ?AbstractLogger $queryLog;

    public function __construct(PDO $pdo, Schema $schema, string $baseUrl) {
        $this->pdo    = $pdo;
        $this->repo   = new RepoDb($baseUrl, $schema, new Schema(new \stdClass()), $this->pdo);
        $this->schema = $schema;
    }

    /**
     * 
     * @param array<string, float> $weights
     * @param float $defaultWeight
     * @return self
     */
    public function setPropertyWeights(array $weights,
                                       float $defaultWeight = 1.0): self {
        $this->propWeights       = $weights;
        $this->propDefaultWeight = $defaultWeight;
        return $this;
    }

    /**
     * Results with a matching weight are 
     * @param array<object> $facets
     * @return self
     */
    public function setWeightedFacets(array $facets, float $defaultWeight = 1.0): self {
        foreach ($facets as $i) {
            $i->weights ??= null;
            $i->type    ??= '';
            if (is_object($i->weights)) {
                $i->weights = (array) $i->weights;
            }
        }
        unset($i);
        $this->facets              = $facets;
        $this->facetsDefaultWeight = $defaultWeight;
        return $this;
    }

    /**
     * 
     * @param array<string, object> $facets
     * @return self
     */
    public function setRangeFacets(array $facets): self {
        $this->rangeFacets = $facets;
        return $this;
    }

    public function setDateFacetsFormat(string $format): self {
        $this->dateFacetsFormat = $format;
        return $this;
    }

    public function setExactWeight(float $weight): self {
        $this->exactWeight = $weight;
        return $this;
    }

    public function setLangWeight(float $weight): self {
        $this->langWeight = $weight;
        return $this;
    }

    /**
     * 
     * @param array<string> $values
     * @param string $property
     * @return self
     */
    public function setNamedEntityFilter(array $values,
                                         string $property = RDF::RDF_TYPE): self {
        $this->namedEntitiesValues   = $values;
        $this->namedEntitiesProperty = $property;
        return $this;
    }

    /**
     * 
     * @param array<string, float> $weights
     * @param float $defaultWeight
     * @return self
     */
    public function setNamedEntityWeights(array $weights,
                                          float $defaultWeight = 1.0): self {
        $this->namedEntityWeights       = $weights;
        $this->namedEntityDefaultWeight = $defaultWeight;
        return $this;
    }

    public function setQueryLog(AbstractLogger $log): self {
        $this->queryLog = $log;
        $this->repo->setQueryLog($log);
        return $this;
    }

    /**
     * 
     * @param string $phrase
     * @param string $language
     * @param bool $inBinary
     * @param array<string> $allowedProperties
     * @param array<SearchTerm> $searchTerms
     * @param null|SearchTerm $spatialTerm
     * @param array<int> $parentIds
     * @return void
     */
    public function search(string $phrase, string $language = '',
                           bool $inBinary = true, array $allowedProperties = [],
                           array $searchTerms = [],
                           ?SearchTerm $spatialTerm = null,
                           array $parentIds = []): void {
        $baseUrl           = $this->repo->getBaseUrl();
        $idProp            = $this->repo->getSchema()->id;
        $linkNamedEntities = count($this->namedEntitiesValues) > 0;
        $this->phrase      = $phrase;
        $query             = "WITH\n";
        $param             = [];

        // FILTERS
        $filters = '';
        if (count($searchTerms) + count($parentIds) > 0) {
            $filters = "AND EXISTS (SELECT 1 FROM filters WHERE id = s.id)";
            $query   .= "filters AS (\nSELECT id FROM\n";
            $n       = 0;
            foreach ($searchTerms as $st) {
                /* @var $st SearchTerm */
                $qp    = $st->getSqlQuery($baseUrl, $idProp, []);
                $query .= $n > 0 ? "JOIN (" : "(";
                $query .= $qp->query;
                $query .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
                $param = array_merge($param, $qp->param);
                $n++;
            }
            if (count($parentIds) > 0) {
                $query .= $n > 0 ? "JOIN (" : "(";
                foreach ($parentIds as $m => $id) {
                    $query .= $m > 0 ? "UNION\n" : "";
                    $query .= "SELECT id FROM get_relatives(?, ?, 999999, 0, false, false)\n";
                    $param = array_merge($param, [$id, $this->schema->parent]);
                }
                $query .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
            }
            $query .= "),\n";
        }

        // WEIGHTS
        $facetsFrom      = "";
        $facetsFromParam = [];
        $facetsSelect    = "";
        $orderExp        = "sum(weight) DESC";
        $weightExp       = "weight_m";
        $weightExpParam  = [];
        if (count($this->propWeights) > 0) {
            $facetsFrom       .= "LEFT JOIN weights_p ON s.property = weights_p.value\n";
            $weightExp        .= " * coalesce(weight_p, ?)";
            $weightExpParam[] = $this->propDefaultWeight;

            $queryTmp = $this->getWeightsWith($this->propWeights, 'weight_p');
            $query    .= "weights_p $queryTmp->query,\n";
            $param    = array_merge($param, $queryTmp->param);
        }
        $mn = '0';
        foreach ($this->facets as $facet) {
            $srcTab  = 'metadata';
            $valCol  = 'value';
            $valType = 'text';
            if ($facet->type === 'object') {
                $srcTab  = 'relations';
                $valCol  = 'target_id';
                $valType = 'bigint';
            }
            $facetsFrom        .= "LEFT JOIN $srcTab meta_$mn ON s.id = meta_$mn.id AND meta_$mn.property = ?\n";
            $facetsFromParam[] = $facet->property;
            if (is_array($facet->weights) && count($facet->weights) > 0) {
                $facetsFrom       .= "LEFT JOIN weights_$mn ON meta_$mn.$valCol = weights_$mn.value\n";
                $facetsSelect     .= ", meta_$mn.$valCol AS meta_$mn, weight_$mn";
                $orderExp         .= ", min(weight_$mn) NULLS LAST";
                $weightExp        .= " * coalesce(weight_$mn, ?)";
                $weightExpParam[] = $this->facetsDefaultWeight;

                $queryTmp = $this->getWeightsWith($facet->weights, "weight_$mn", $valType);
                $query    .= "weights_$mn $queryTmp->query,\n";
                $param    = array_merge($param, $queryTmp->param);
            } elseif (!is_array($facet->weights) && !empty($facet->weights)) {
                $facetsSelect .= ", CASE WHEN meta_$mn.value_t IS NOT NULL THEN to_char(meta_$mn.value_t, '" . $this->dateFacetsFormat . "') ELSE meta_$mn.value END AS meta_$mn";
                $desc         = strtolower($facet->weights) === 'desc';
                $orderExp     .= ", " . ($desc ? "max" : "min") . "(meta_$mn)" . ($desc ? " DESC" : "") . " NULLS LAST";
            } else {
                $facetsSelect .= ", meta_$mn.$valCol AS meta_$mn";
            }
            $mn = (string) (((int) $mn) + 1);
        }

        // INITIAL SEARCH
        $propsFilter = '';
        $propsParam  = [];
        if (empty($phrase) && $spatialTerm !== null) {
            // SPATIAL-ONLY SEARCH
            if (count($allowedProperties) > 0) {
                $propsFilter = "AND CASE 
                    WHEN m.property IS NOT NULL THEN m.property 
                        ELSE 'BINARY' 
                    END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
                $propsParam  = $allowedProperties;
            }
            $qp        = $spatialTerm->getSqlQuery($baseUrl, $idProp, []);
            $qp->query = (string) preg_replace('/^.*FROM/sm', '', $qp->query);
            $inBinaryF = $inBinary ? "" : "AND ss.id IS NULL";
            $query     .= "
                search1 AS (
                    SELECT
                        coalesce(ss.id, m.id) AS id,
                        -1::bigint AS ftsid,
                        CASE
                            WHEN m.property IS NOT NULL THEN m.property
                            ELSE 'BINARY'
                        END AS property,
                        1.0 AS weight_m,
                        null::text AS link_property
                    FROM $qp->query $inBinaryF $propsFilter
                )
            ";
            $param     = array_merge($param, $qp->param, $propsParam);
            $curTab    = 'search1';
        } elseif (empty($phrase) && $spatialTerm === null && count($searchTerms) > 0) {
            $query   .= "
                search1 AS (
                    SELECT 
                        id, 
                        -1::bigint AS ftsid,
                        null::text AS property,
                        1.0 AS weight_m,
                        null::text AS link_property
                    FROM filters
                )
            ";
            $curTab  = 'search1';
            $filters = "";
        } else {
            if (count($allowedProperties) > 0) {
                $propsFilter = "AND CASE 
                    WHEN sm.property IS NOT NULL THEN sm.property 
                        WHEN f.iid IS NOT NULL THEN ? 
                        ELSE 'BINARY' 
                    END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
                $propsParam  = array_merge([$this->schema->id], $allowedProperties);
            }
            $inBinaryF = $inBinary ? "" : "AND f.id IS NULL";
            $langMatch = !empty($language) ? "sm.lang = ?" : "?::bool";
            $langParam = !empty($language) ? $language : 0.0;
            $query     .= "
                search1 AS (
                    SELECT
                        coalesce(f.id, f.iid, sm.id) AS id,
                        ftsid,
                        CASE
                            WHEN sm.property IS NOT NULL THEN sm.property
                            WHEN f.iid IS NOT NULL THEN ?
                            ELSE 'BINARY'
                        END AS property,
                        CASE WHEN raw = ? THEN ? ELSE 1.0 END * CASE WHEN $langMatch THEN ? ELSE 1.0 END AS weight_m,
                        null::text AS link_property
                    FROM
                        full_text_search f
                        LEFT JOIN metadata sm using (mid)
                    WHERE
                        websearch_to_tsquery('simple', ?) @@ segments
                        $inBinaryF
                        $propsFilter
                )
            ";
            $param     = array_merge(
                $param,
                [
                    $this->schema->id,
                    $phrase, $this->exactWeight, $langParam, $this->langWeight,
                    SearchTerm::escapeFts($phrase)
                ],
                                          $propsParam
            );
            $curTab    = 'search1';

            // SPATIAL SEARCH
            if ($spatialTerm !== null) {
                $qp        = $spatialTerm->getSqlQuery($baseUrl, $idProp, []);
                $inBinaryF = $inBinary ? '' : 'AND ss.id IS NULL';
                $query     .= ",
                    search1s AS (
                        SELECT *
                        FROM
                            $curTab
                            JOIN ($qp->query $inBinaryF) t USING (id)
                    )
                ";
                $param     = array_merge($param, $qp->param);
                $curTab    = 'search1s';
            }
        }

        // LINK TO NAMED ENTITIES
        if ($this->linkNamedEntities()) {
            $curTab           = 'search2';
            $neIn             = substr(str_repeat('?, ', count($this->namedEntitiesValues)), 0, -2);
            $query            .= ",
                search2 AS (
                    SELECT id, ftsid, property, weight_m, NULL::text AS link_property
                    FROM search1
                  UNION
                    SELECT r.id, s.ftsid, s.property, s.weight_m, r.property AS link_property
                    FROM
                        (
                            SELECT DISTINCT ON (id) * 
                            FROM 
                                search1 s 
                                LEFT JOIN weights_p w ON s.property = w.value 
                            ORDER BY id, coalesce(weight_p, ?) * weight_m DESC
                        ) s
                        JOIN metadata mne ON s.id = mne.id AND mne.property = ? AND mne.value IN ($neIn)
                        JOIN relations r ON s.id = r.target_id
                )
            ";
            $param            = array_merge(
                $param,
                [$this->propDefaultWeight, $this->namedEntitiesProperty],
                $this->namedEntitiesValues
            );
            $weightExp        .= " * coalesce(weight_ne, ?)";
            $weightExpParam[] = $this->namedEntityDefaultWeight;
            $facetsFrom       .= "LEFT JOIN weights_ne wne ON s.link_property = wne.value\n";

            $queryTmp = $this->getWeightsWith($this->namedEntityWeights, 'weight_ne');
            $query    .= ", weights_ne $queryTmp->query\n";
            $param    = array_merge($param, $queryTmp->param);
        }

        // RANGE FACETS
        foreach ($this->rangeFacets as $i) {
            $minPlch      = substr(str_repeat(', ?', count($i->start)), 2);
            $maxPlch      = substr(str_repeat(', ?', count($i->end)), 2);
            $query        .= ",
                meta_$mn AS (
                    SELECT 
                        id, 
                        numrange('[' || vmin::text || ', ' || vmax::text || ']') AS meta_$mn
                    FROM
                        (
                            SELECT id, min(m.value_n) AS vmin
                            FROM metadata m JOIN $curTab s USING (id)
                            WHERE
                                m.property IN ($minPlch)
                                $filters
                            GROUP BY 1
                        ) t1
                        JOIN (
                            SELECT id, max(m.value_n) AS vmax
                            FROM metadata m JOIN $curTab s USING (id)
                            WHERE
                                m.property IN ($maxPlch)
                                $filters
                            GROUP BY 1
                        ) t2 USING (id)
                )
            ";
            $param        = array_merge($param, $i->start, $i->end);
            $facetsSelect .= ", meta_$mn.meta_$mn AS meta_$mn";
            $facetsFrom   .= "LEFT JOIN meta_$mn ON meta_$mn.id = s.id\n";
            $mn           = (string) (((int) $mn) + 1);
        }

        // WEIGHTS
        $query = "CREATE TEMPORARY TABLE " . self::TEMPTABNAME . " AS 
            $query
            SELECT 
                s.id, s.ftsid, s.property, s.link_property, 
                $weightExp AS weight
                $facetsSelect
            FROM
                $curTab s
                $facetsFrom
            WHERE
                $weightExp > 0
                $filters
        ";
        $param = array_merge(
            $param,
            $weightExpParam,
            $facetsFromParam,
            $weightExpParam
        );

        if ($_POST['debug'] ?? false) {
            exit(new QueryPart($query, $param));
        }
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        if (isset($this->queryLog)) {
            $this->queryLog->debug((string) (new QueryPart($query, $param)));
        }
        $this->pdo->beginTransaction();
        $query = $this->pdo->prepare($query);
        $query->execute($param);

        $this->orderExp = $orderExp;
    }

    /**
     * 
     * @param int $page
     * @param int $pageSize
     * @param SearchConfig $config
     * @return Generator<object>
     */
    public function getSearchPage(int $page, int $pageSize, SearchConfig $config): Generator {
        $config->skipArtificialProperties = true;

        $offset = $page * $pageSize;
        $query  = "
            CREATE TEMPORARY TABLE _page AS 
            SELECT id, sum(weight) AS weight
            FROM " . self::TEMPTABNAME . "
            GROUP BY 1
            ORDER BY $this->orderExp
            OFFSET ? 
            LIMIT ?
        ";
        $param  = [$offset, $pageSize];
        if (isset($this->queryLog)) {
            $this->queryLog->debug(new QueryPart($query, $param));
        }
        $query = $this->pdo->prepare($query);
        $query->execute($param);

        // technical triples
        $phraseEsc = SearchTerm::escapeFts($this->phrase);
        $query     = "
            SELECT id, property, type, lang, value
            FROM (
                -- total count
                SELECT null::bigint AS id, ? AS property, ? AS type, '' AS lang, count(DISTINCT id)::text AS value, 0::bigint AS ftsid
                FROM " . self::TEMPTABNAME . "
              UNION
                -- order
                SELECT id, ? AS property, ? AS type, '' AS lang, (row_number() OVER ())::text AS value, 0::bigint AS ftsid
                FROM _page
              UNION
                -- match highlighting
                SELECT
                    p.id, ? AS property, ? AS type, '' AS lang, ts_headline('simple', raw, websearch_to_tsquery('simple', ?), ?) AS value, ftsid
                FROM 
                    _page p
                    JOIN " . self::TEMPTABNAME . " USING (id)
                    JOIN full_text_search USING (ftsid)
              UNION
                -- match boolean properties - here store property with the match
                SELECT id, ? AS property, ? AS type, '' AS lang, coalesce(link_property, property) AS value, ftsid
                FROM
                    _page
                    JOIN " . self::TEMPTABNAME . " USING (id)
                WHERE coalesce(link_property, property) IS NOT NULL
              UNION
                -- match weights
                SELECT id, ? AS property, ? AS type, '' AS lang, weight::text AS value, 0::bigint AS ftsid
                FROM _page
            ) t
            -- to enable pairing highlights and properties where it happened
            ORDER BY ftsid
        ";
        $param     = [
            $this->schema->searchCount, RDF::XSD_INTEGER,
            $this->schema->searchOrder, RDF::XSD_INTEGER,
            $this->schema->searchFts, RDF::XSD_STRING, $phraseEsc, $config->getTsHeadlineOptions(),
            $this->schema->searchMatch, RDF::XSD_ANY_URI,
            $this->schema->searchWeight, RDF::XSD_FLOAT,
        ];
        if (isset($this->queryLog)) {
            $this->queryLog->debug(new QueryPart($query, $param));
        }
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        while ($row   = $query->fetchObject()) {
            yield $row;
        }

        // metadata of matched resources
        $query = "SELECT id FROM _page";
        $query = $this->repo->getPdoStatementBySqlQuery($query, [], $config);
        while ($row   = $query->fetchObject()) {
            yield $row;
        }
    }

    /**
     * 
     * @param string $prefLang
     * @return array<string, array<string, mixed>>
     */
    public function getSearchFacets(string $prefLang = ''): array {
        $stats = [];

        // MATCH PROPERTY
        $query  = $this->pdo->query("
            SELECT 
                property AS value, 
                property AS label, 
                count(DISTINCT id) AS count 
            FROM " . self::TEMPTABNAME . "
            WHERE property IS NOT NULL
            GROUP BY 1 
            ORDER BY 2 DESC
        ");
        $values = $query->fetchAll(PDO::FETCH_OBJ);
        if (count($values) > 0) {
            $stats['property'] = [
                'continues' => false,
                'values'    => $query->fetchAll(PDO::FETCH_OBJ),
            ];
        }

        // LINK PROPERTY
        if ($this->linkNamedEntities()) {
            $query                 = $this->pdo->query("
                SELECT
                    link_property AS value,
                    link_property AS label, 
                    count(DISTINCT id) AS count
                FROM " . self::TEMPTABNAME . "
                WHERE link_property IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
            ");
            $stats['linkProperty'] = [
                'continues' => false,
                'values'    => $query->fetchAll(PDO::FETCH_OBJ),
            ];
        }

        // FACETS
        $mn = 0;
        foreach ($this->facets as $facet) {
            if ($facet->type === 'object') {
                $query = $this->pdo->prepare("
                    SELECT * 
                    FROM (
                        SELECT DISTINCT ON (m.id)
                            ? || m.id::text AS value,
                            m.value AS label,
                            count
                        FROM
                            (
                                SELECT meta_$mn, count(DISTINCT id) AS count 
                                FROM " . self::TEMPTABNAME . " 
                                GROUP BY 1
                            ) t
                            JOIN metadata m ON meta_$mn = m.id
                        WHERE
                            m.property = ?
                        ORDER BY m.id, m.lang = ? DESC
                    ) t
                    ORDER BY 2 DESC, 1
                ");
                $param = [$this->repo->getBaseUrl(), $this->schema->label, $prefLang];
                $query->execute($param);
            } else {
                $query = $this->pdo->query("
                    SELECT
                        meta_$mn AS value,
                        meta_$mn AS label,
                        count(DISTINCT id) AS count 
                    FROM " . self::TEMPTABNAME . "
                    WHERE meta_$mn IS NOT NULL
                    GROUP BY 1 
                    ORDER BY 2 DESC
                ");
            }
            $stats[$facet->property] = [
                'values'    => [],
                'continues' => is_string($facet->weights),
            ];
            while ($row                     = $query->fetchObject()) {
                $row->value                          = is_numeric($row->value) ? (float) $row->value : $row->value;
                $stats[$facet->property]['values'][] = $row;
            }
            $mn++;
        }

        // RANGE FACETS
        foreach ($this->rangeFacets as $fid => $facet) {
            $param = [];

            if ($facet->precision === 0) {
                $stepExpr = "CASE WHEN range > least(?, nd) THEN range / least(?, nd) ELSE 1 END";
                $param    = array_merge($param, array_fill(0, 4, $facet->bins));
                $filter   = "OR step = 1";
            } else {
                $stepExpr = "range / least(?, nd)";
                $param    = array_merge($param, array_fill(0, 2, $facet->bins));
                $filter   = "";
            }
            $query       = "
                WITH
                    limits AS (
                        SELECT 
                            min(lower(meta_$mn)) AS start,
                            max(upper(meta_$mn)) AS stop,
                            (max(upper(meta_$mn)) - min(lower(meta_$mn))) AS range,
                            count(DISTINCT meta_$mn) AS nd
                        FROM " . self::TEMPTABNAME . "
                    ),
                    steps AS (
                        SELECT 
                            $stepExpr AS step,
                            round(generate_series(start, stop, $stepExpr), ?) AS steps
                        FROM limits
                    ),
                    bins AS (
                        SELECT
                            numrange('[' || t2.start::text || ', ' || CASE WHEN row = 1 THEN limits.stop::text || ']' ELSE t2.stop::text || ')' END) AS bin
                        FROM
                            limits,
                            (
                                SELECT start, stop, row_number() OVER (ORDER BY start DESC) AS row
                                FROM (
                                    SELECT
                                        step, 
                                        steps AS start, 
                                        lead(steps) OVER () AS stop
                                    FROM steps
                                ) t1
                                WHERE stop IS NOT NULL $filter
                            ) t2
                    )
                SELECT 
                    null AS value, 
                    bin::text AS label, 
                    count(DISTINCT id) AS count
                FROM
                    bins b
                    JOIN " . self::TEMPTABNAME . " m ON b.bin && m.meta_$mn
                GROUP BY b.bin
                ORDER BY lower(bin)
            ";
            $param       = array_merge($param, [$facet->precision]);
            $query       = $this->pdo->prepare($query);
            $query->execute($param);
            $stats[$fid] = [
                'continues' => true,
                'values'    => $query->fetchAll(PDO::FETCH_OBJ),
            ];
            $mn++;
        }

        return $stats;
    }

    public function closeSearch(): void {
        $this->pdo->rollBack();
    }

    /**
     * 
     * @param array<string, float> $weights
     * @param string $weightName
     * @param string $valueType
     * @return QueryPart
     */
    private function getWeightsWith(array $weights, string $weightName,
                                    string $valueType = 'text'): QueryPart {
        $query = new QueryPart("(value, $weightName) AS (VALUES ");
        foreach ($weights as $k => $v) {
            $query->query   .= "(?::$valueType, ?::float),";
            $query->param[] = $k;
            $query->param[] = $v;
        }
        $query->query = substr($query->query, 0, -1) . ")";
        return $query;
    }

    private function linkNamedEntities(): bool {
        return count($this->namedEntitiesValues) > 0;
    }
}
