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
use Psr\Log\AbstractLogger;
use zozlak\queryPart\QueryPart;
use zozlak\RdfConstants as RDF;

/**
 * Provides an API for advanced weighted 
 *
 * @author zozlak
 */
class SmartSearch {

    const TEMPTABNAME      = "_matches";
    const FACET_MATCH      = 'matchProperty';
    const FACET_LINK       = 'linkProperty';
    const FACET_LITERAL    = 'literal';
    const FACET_OBJECT     = 'object';
    const FACET_CONTINUOUS = 'continuous';
    const FACET_MAP        = 'map';
    const FACET_DISCRETE   = [self::FACET_LITERAL, self::FACET_OBJECT];
    const FACET_ANY        = [self::FACET_MATCH, self::FACET_LINK, self::FACET_LITERAL,
        self::FACET_OBJECT, self::FACET_CONTINUOUS, self::FACET_MAP];

    private PDO $pdo;
    private RepoDb $repo;
    private Schema $schema;

    /**
     * 
     * @var array<object>
     */
    private array $facets = [];
    private object $matchFacet;
    private object $linkFacet;

    /**
     * 
     * @var array<string, object>
     */
    private float $exactWeight = 2.0;
    private float $langWeight  = 1.5;
    private string $phrase;
    private ?AbstractLogger $queryLog    = null;

    public function __construct(PDO $pdo, Schema $schema, string $baseUrl) {
        $this->pdo        = $pdo;
        $this->repo       = new RepoDb($baseUrl, $schema, new Schema(new \stdClass()), $this->pdo);
        $this->schema     = $schema;
        $this->matchFacet = (object) ['weights' => [], 'defaultWeight' => 0];
        $this->linkFacet  = (object) ['weights' => [], 'classes' => []];
    }

    /**
     * @param array<object> $facets
     * @return self
     */
    public function setFacets(array $facets, float $defaultWeight = 1.0): self {
        foreach ($facets as $i) {
            if (!in_array($i->type, self::FACET_ANY)) {
                throw new RepoLibException("Unsupported facet type: $i->type");
            }
            $i->defaultWeight ??= $defaultWeight;
            $i->label         ??= $i->property;
            if (is_object($i->label)) {
                $i->label = (array) $i->label;
            } elseif (is_scalar($i->label)) {
                $i->label = ['und' => (string) $i->label];
            }
            $i->weights ??= null;
            if (is_object($i->weights)) {
                $i->weights = (array) $i->weights;
            }
            if (is_array($i->weights) && count($i->weights) === 0) {
                $i->weights = null;
            }
            if ($i->type === self::FACET_CONTINUOUS) {
                $i->distribution ??= false;
                $i->precision    ??= 0;
                $i->start        = is_array($i->start) ? $i->start : [$i->start];
                $i->end          = is_array($i->end) ? $i->end : [$i->end];
            }
            if ($i->type === self::FACET_LINK) {
                $i->classes      ??= [];
                $i->weights      ??= [];
                $this->linkFacet = $i;
            } elseif ($i->type === self::FACET_MATCH) {
                $i->weights       ??= [];
                $this->matchFacet = $i;
            } elseif ($i->type === self::FACET_MAP) {
                $i->property = self::FACET_MAP;
            }
        }
        $this->facets = $facets;
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
                           array $parentIds = [], int $matchesLimit = 10000): void {
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        $baseUrl           = $this->repo->getBaseUrl();
        $idProp            = $this->repo->getSchema()->id;
        $linkNamedEntities = count($this->linkFacet->classes) > 0;
        $this->phrase      = $phrase;
        // search conditions based on FTS index or spatial index
        $indexSearch       = !(empty($phrase) && $spatialTerm === null);
        // search conditions base on search terms
        $filteredSearch    = count($searchTerms) + count($parentIds) > 0;

        // FILTERS
        $filterExp = '';
        if ($filteredSearch) {
            $filterExp   = "WHERE EXISTS (SELECT 1 FROM filters WHERE id = s.id)";
            $filterQuery = "CREATE TEMPORARY TABLE filters AS (\nSELECT id FROM\n";
            $filterParam = [];
            $n           = 0;
            foreach ($searchTerms as $st) {
                /* @var $st SearchTerm */
                $tmpQuery    = $st->getSqlQuery($baseUrl, $idProp, []);
                $filterQuery .= $n > 0 ? "JOIN (" : "(";
                $filterQuery .= $tmpQuery->query;
                $filterQuery .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
                $filterParam = array_merge($filterParam, $tmpQuery->param);
                $n++;
            }
            if (count($parentIds) > 0) {
                $filterQuery .= $n > 0 ? "JOIN (" : "(";
                foreach ($parentIds as $m => $id) {
                    $filterQuery .= $m > 0 ? "UNION\n" : "";
                    $filterQuery .= "SELECT id FROM get_relatives(?, ?, 999999, 0, false, false) WHERE n <> 0\n";
                    $filterParam = array_merge($filterParam, [$id, $this->schema->parent]);
                }
                $filterQuery .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
            }
            $filterQuery .= ")\n";
            $this->queryLog?->debug((string) (new QueryPart($filterQuery, $filterParam)));
            $t           = microtime(true);
            $filterQuery = $this->pdo->prepare($filterQuery);
            $filterQuery->execute($filterParam);
            $this->queryLog?->debug('Execution time ' . microtime(true) - $t);
            unset($filterQuery, $filterParam, $tmpQuery);
        }

        $searchQuery = "CREATE TEMPORARY TABLE search9 AS\nWITH\n";
        $searchParam = [];
        $matchQuery  = "CREATE TEMPORARY TABLE " . self::TEMPTABNAME . " AS WITH\n";
        $matchParam  = [];
        $tmpQuery    = null;

        // WEIGHTS - watch out - some go to search query, some to match query and some to both
        if (count($this->matchFacet->weights) > 0) {
            $tmpQuery    = $this->getWeightsWith($this->matchFacet->weights, 'weight_p');
            $searchQuery .= "weights_p $tmpQuery->query,\n";
            $searchParam = array_merge($searchParam, $tmpQuery->param);
            $matchQuery  .= "weights_p $tmpQuery->query,\n";
            $matchParam  = array_merge($matchParam, $tmpQuery->param);
        }
        foreach ($this->facets as $mn => $facet) {
            if (is_array($facet->weights) && !($facet === $this->matchFacet || $facet === $this->linkFacet)) {
                $tmpQuery   = $this->getWeightsWith($facet->weights, "weight_$mn", $facet->type === 'object' ? 'bigint' : 'text');
                $matchQuery .= "weights_$mn $tmpQuery->query,\n";
                $matchParam = array_merge($matchParam, $tmpQuery->param);
            }
        }
        if ($linkNamedEntities) {
            $tmpQuery    = $this->getWeightsWith($this->linkFacet->weights, 'weight_ne');
            $searchQuery .= "weights_ne $tmpQuery->query,\n";
            $searchParam = array_merge($searchParam, $tmpQuery->param);
        }
        $matchQuery = substr($matchQuery, 0, -2) . "\n"; // get rid of final coma
        // INITIAL SEARCH
        if (!$indexSearch && $filteredSearch) {
            $searchQuery .= "
                search1 AS (
                    SELECT 
                        id, 
                        -1::bigint AS ftsid,
                        null::text AS property,
                        1.0 AS weight_m
                    FROM filters
                )
            ";
            $curTab      = 'search1';
            $filterExp   = '';
        } elseif (empty($phrase) && $spatialTerm !== null) {
            // SPATIAL-ONLY SEARCH
            $propsFilter = '';
            $propsParam  = [];
            if (count($allowedProperties) > 0) {
                $propsFilter = "AND CASE 
                    WHEN m.property IS NOT NULL THEN m.property 
                        ELSE 'BINARY' 
                    END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
                $propsParam  = $allowedProperties;
            }
            $tmpQuery        = $spatialTerm->getSqlQuery($baseUrl, $idProp, []);
            $tmpQuery->query = (string) preg_replace('/^.*FROM/sm', '', $tmpQuery->query);
            $inBinaryF       = $inBinary ? "" : "AND ss.id IS NULL";
            $searchQuery     .= "
                search1 AS (
                    SELECT
                        coalesce(ss.id, m.id) AS id,
                        -1::bigint AS ftsid,
                        CASE
                            WHEN m.property IS NOT NULL THEN m.property
                            ELSE 'BINARY'
                        END AS property,
                        1.0 AS weight_m
                    FROM $tmpQuery->query $inBinaryF $propsFilter
                )
            ";
            $searchParam     = array_merge($searchParam, $tmpQuery->param, $propsParam);
            $curTab          = 'search1';
        } else {
            $propsFilter = '';
            $propsParam  = [];
            if (count($allowedProperties) > 0) {
                $propsFilter = "AND CASE 
                    WHEN sm.property IS NOT NULL THEN sm.property 
                        WHEN f.iid IS NOT NULL THEN ? 
                        ELSE 'BINARY' 
                    END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
                $propsParam  = array_merge([$this->schema->id], $allowedProperties);
            }
            $inBinaryF   = $inBinary ? "" : "AND f.id IS NULL";
            $langMatch   = !empty($language) ? "sm.lang = ?" : "?::bool";
            $langParam   = !empty($language) ? $language : 0.0;
            $searchQuery .= "
                search1 AS (
                    SELECT
                        coalesce(f.id, f.iid, sm.id) AS id,
                        ftsid,
                        CASE
                            WHEN sm.property IS NOT NULL THEN sm.property
                            WHEN f.iid IS NOT NULL THEN ?
                            ELSE 'BINARY'
                        END AS property,
                        CASE WHEN raw = ? THEN ? ELSE 1.0 END * CASE WHEN $langMatch THEN ? ELSE 1.0 END AS weight_m
                    FROM
                        full_text_search f
                        LEFT JOIN metadata sm using (mid)
                    WHERE
                        websearch_to_tsquery('simple', ?) @@ segments
                        $inBinaryF
                        $propsFilter
                )
            ";
            $searchParam = array_merge(
                $searchParam,
                [
                    $this->schema->id,
                    $phrase, $this->exactWeight, $langParam, $this->langWeight,
                    SearchTerm::escapeFts($phrase)
                ],
                $propsParam
            );
            $curTab      = 'search1';

            // SPATIAL SEARCH
            if ($spatialTerm !== null) {
                $tmpQuery    = $spatialTerm->getSqlQuery($baseUrl, $idProp, []);
                $inBinaryF   = $inBinary ? '' : 'AND ss.id IS NULL';
                $searchQuery .= ",
                    search1s AS (
                        SELECT *
                        FROM
                            $curTab
                            JOIN ($tmpQuery->query $inBinaryF) t USING (id)
                    )
                ";
                $searchParam = array_merge($searchParam, $tmpQuery->param);
                $curTab      = 'search1s';
            }
        }

        if (!$linkNamedEntities) {
            //$searchQuery   .= "SELECT * FROM $curTab s $filterExp ORDER BY weight_m DESC LIMIT ?\n";
            //$searchParam[] = $matchesLimit;
            $searchQuery  .= "SELECT * FROM $curTab s $filterExp\n";
            $matchQuery   .= "
                SELECT 
                    s.id, s.ftsid, s.property, 
                    null::text AS link_property, null::text AS facet, null::text AS value, 
                    weight_m * coalesce(weight_p, ?) AS weight
                FROM
                    search9 s
                    LEFT JOIN weights_p w ON s.property = w.value
            ";
            $matchParam[] = $this->matchFacet->defaultWeight;
        } else {
            // LINK TO NAMED ENTITIES
            $neIn        = substr(str_repeat('?, ', count($this->linkFacet->classes)), 0, -2);
            $searchQuery .= "
                SELECT * FROM (
                    SELECT 
                        id, 
                        ftsid, 
                        property, 
                        NULL::text AS link_property, 
                        weight_m * coalesce(weight_p, ?) AS weight
                    FROM
                        $curTab s
                        LEFT JOIN weights_p w ON s.property = w.value
                    $filterExp
                  UNION
                    SELECT 
                        s.id, 
                        t.ftsid, 
                        t.property, 
                        s.property AS link_property, 
                        t.weight_m * coalesce(t.weight_p, ?) * coalesce(wne.weight_ne, ?) AS weight
                    FROM
                        (
                            SELECT DISTINCT ON (id) * 
                            FROM 
                                $curTab s 
                                LEFT JOIN weights_p w ON s.property = w.value 
                            ORDER BY id, coalesce(weight_p, ?) * weight_m DESC
                        ) t
                        JOIN metadata mne ON t.id = mne.id AND mne.property = ? AND mne.value IN ($neIn)
                        JOIN relations s ON t.id = s.target_id
                        LEFT JOIN weights_ne wne ON s.property = wne.value
                    $filterExp
                ) t 
            ";
            //    ORDER BY weight DESC
            //    LIMIT ?
            $searchParam = array_merge(
                $searchParam,
                [
                    $this->matchFacet->defaultWeight, // first union part
                    $this->matchFacet->defaultWeight, // select of second union part
                    $this->linkFacet->defaultWeight, // select of second union part
                    $this->matchFacet->defaultWeight, // subselect of second union part
                    $this->linkFacet->property // join with mne
                ],
                $this->linkFacet->classes, // join with mne
                //[$matchesLimit],
            );
            $matchQuery  .= "SELECT id, ftsid, property, link_property, null::text AS facet, null::text AS value, weight FROM search9\n";
        }
        $this->queryLog?->debug((string) (new QueryPart($searchQuery, $searchParam)));
        $t           = microtime(true);
        $searchQuery = $this->pdo->prepare($searchQuery);
        $searchQuery->execute($searchParam);
        $this->queryLog?->debug('Execution time ' . microtime(true) - $t);
        unset($searchQuery, $searchParam, $tmpQuery);

        // ORDINARY FACETS DATA
        foreach ($this->facets as $mn => $facet) {
            if (!in_array($facet->type, self::FACET_DISCRETE)) {
                continue;
            }
            $srcTab = 'metadata';
            $valCol = 'value';
            if ($facet->type === 'object') {
                $srcTab = 'relations';
                $valCol = 'target_id';
            }
            $weightQuery = '';
            $weightValue = 'null::float';
            if (is_array($facet->weights)) {
                $weightQuery  = "LEFT JOIN weights_$mn w ON m.$valCol = w.value";
                $weightValue  = "coalesce(w.weight_$mn, ?)";
                $matchParam[] = $facet->defaultWeight;
            }
            $matchQuery   .= "UNION
                SELECT
                    s.id, 
                    null::bigint as fstid, 
                    null::text AS property, 
                    null::text AS link_property, 
                    m.property AS facet, 
                    m.$valCol::text AS value,
                    $weightValue AS weight
                FROM 
                    search9 s 
                    JOIN $srcTab m ON s.id = m.id AND m.property = ?
                    $weightQuery
            ";
            $matchParam[] = $facet->property;
        }
        // CONTINUOUS FACETS DATA
        $rangeFilterExp = $filterExp !== '' ? 'AND' . substr($filterExp, 5) : '';
        foreach ($this->facets as $facet) {
            if ($facet->type !== self::FACET_CONTINUOUS) {
                continue;
            }
            $minPlch    = substr(str_repeat(', ?', count($facet->start)), 2);
            $maxPlch    = substr(str_repeat(', ?', count($facet->end)), 2);
            $matchQuery .= "UNION
                SELECT
                    s.id,
                    null::bigint AS ftsid,
                    null::text AS property,
                    null::text AS link_property,
                    ?::text AS facet, 
                    '[' || vmin::text || ', ' || vmax::text || ']' AS value,
                    null::float AS weight
                FROM
                    search9 s
                    JOIN (
                        SELECT id, min(m.value_n) AS vmin
                        FROM metadata m JOIN search9 s USING (id)
                        WHERE
                            m.property IN ($minPlch)
                            $rangeFilterExp
                        GROUP BY 1
                    ) t1 USING (id)
                    JOIN (
                        SELECT id, max(m.value_n) AS vmax
                        FROM metadata m JOIN search9 s USING (id)
                        WHERE
                            m.property IN ($maxPlch)
                            $rangeFilterExp
                        GROUP BY 1
                    ) t2 USING (id)
            ";
            $matchParam = array_merge($matchParam, [$facet->property], $facet->start, $facet->end);
        }

        $this->queryLog?->debug((string) (new QueryPart($matchQuery, $matchParam)));
        $this->pdo->beginTransaction();
        $t          = microtime(true);
        $matchQuery = $this->pdo->prepare($matchQuery);
        $matchQuery->execute($matchParam);
        $this->queryLog?->debug('Execution time ' . (microtime(true) - $t));

        $query = "DELETE FROM " . self::TEMPTABNAME . " WHERE id IN (SELECT id FROM " . self::TEMPTABNAME . " WHERE weight = 0)";
        $this->queryLog?->debug($query);
        $t     = microtime(true);
        $this->pdo->query($query);
        $this->queryLog?->debug('Execution time ' . microtime(true) - $t);
    }

    /**
     * 
     * @param int $page
     * @param int $pageSize
     * @param SearchConfig $config
     * @return Generator<object>
     */
    public function getSearchPage(int $page, int $pageSize,
                                  SearchConfig $config, string $prefLang): Generator {
        $config->skipArtificialProperties = true;

        $param    = [];
        $oGroupBy = $oOrderBy = '';
        if (count($config->orderBy) > 0) {
            $orderBy = reset($config->orderBy);
            $oAsc    = 'ASC';
            if (substr($orderBy, 0, 1) === '^') {
                $orderBy = substr($orderBy, 1);
                $oAsc    = 'DESC';
            }
            $oQuery   = "
                LEFT JOIN (
                    SELECT
                        id, 
                        min(value) FILTER (WHERE lang = ?) AS o1,
                        min(value) AS o2
                    FROM metadata m 
                    WHERE
                        m.property = ?
                        AND EXISTS (SELECT 1 FROM " . self::TEMPTABNAME . " WHERE id = m.id)
                    GROUP BY 1
                ) o USING (id)
            ";
            $oGroupBy = ', o1, o2';
            $oOrderBy = ", COALESCE(o1, o2) $oAsc NULLS LAST";
            $param    = [$prefLang, $orderBy];
        }
        $offset  = $page * $pageSize;
        $query   = "
            CREATE TEMPORARY TABLE _page AS 
            SELECT id, round(exp(sum(ln(weight)))) AS weight
            FROM
                (
                    SELECT id, property, max(weight) AS weight
                    FROM _matches
                    GROUP BY 1, 2
                ) w
                $oQuery
            GROUP BY 1 $oGroupBy
            ORDER BY round(exp(sum(ln(weight)))) DESC NULLS LAST $oOrderBy
            OFFSET ? 
            LIMIT ?
        ";
        $param[] = $offset;
        $param[] = $pageSize;
        $this->queryLog?->debug(new QueryPart($query, $param));
        $t       = microtime(true);
        $query   = $this->pdo->prepare($query);
        $query->execute($param);
        $this->queryLog?->debug('Execution time ' . (microtime(true) - $t));

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
        $t         = microtime(true);
        $this->queryLog?->debug(new QueryPart($query, $param));
        $query     = $this->pdo->prepare($query);
        $query->execute($param);
        $this->queryLog?->debug('Execution time ' . (microtime(true) - $t));
        while ($row       = $query->fetchObject()) {
            yield $row;
        }

        // metadata of matched resources
        $query = "SELECT id FROM _page";
        $t     = microtime(true);
        $query = $this->repo->getPdoStatementBySqlQuery($query, [], $config);
        $this->queryLog?->debug('Execution time ' . (microtime(true) - $t));
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
        $t     = microtime(true);
        $t1 = $t;

        // FACETS
        $objectFacets  = $literalFacets = [];
        foreach ($this->facets as $facet) {
            if ($facet->type === self::FACET_OBJECT) {
                $objectFacets[] = $facet->property;
            } elseif ($facet->type === self::FACET_LITERAL) {
                $literalFacets[] = $facet->property;
            }
            $tmp         = clone($facet);
            $tmp->values = [];
            if ($facet === $this->linkFacet || $facet === $this->matchFacet) {
                $tmp->property = $tmp->type;
            }
            $stats[$tmp->property] = $tmp;
        }
        // object facets
        if (count($objectFacets) > 0) {
            $query = $this->pdo->prepare("
                SELECT facet, value, label, count
                FROM (
                    SELECT DISTINCT ON (facet, m.id)
                        facet,
                        ? || m.id::text AS value,
                        m.value AS label,
                        count
                    FROM
                       (
                           SELECT facet, value::bigint AS value, count(DISTINCT id) AS count 
                           FROM " . self::TEMPTABNAME . " s
                           WHERE facet IN (" . substr(str_repeat(', ?', count($objectFacets)), 2) . ")
                           GROUP BY 1, 2
                       ) t
                       JOIN metadata m ON t.value::bigint = m.id
                    WHERE m.property = ?
                    ORDER BY facet, m.id, m.lang = ? DESC
                ) t
                ORDER BY count DESC, label
            ");
            $param = array_merge(
                [$this->repo->getBaseUrl()],
                                         $objectFacets,
                                         [$this->schema->label, $prefLang]
            );
            $query->execute($param);
            while ($row   = $query->fetchObject()) {
                $facet                   = $row->facet;
                unset($row->facet);
                $stats[$facet]->values[] = $row;
            }
            $this->queryLog?->debug('FACETS STATS (object facets) time ' . (microtime(true) - $t1));
            $t1 = microtime(true);
        }
        // value facets
        if (count($literalFacets) > 0) {
            $query = $this->pdo->prepare("
                SELECT
                    facet,
                    value,
                    value AS label,
                    count(DISTINCT id) AS count 
                FROM " . self::TEMPTABNAME . "
                    WHERE facet IN (" . substr(str_repeat(', ?', count($literalFacets)), 2) . ")
                    GROUP BY 1, 2, 3
                    ORDER BY 1, 4 DESC
            ");
            $query->execute($literalFacets);
            while ($row   = $query->fetchObject()) {
                $row->value              = is_numeric($row->value) ? (float) $row->value : $row->value;
                $facet                   = $row->facet;
                unset($row->facet);
                $stats[$facet]->values[] = $row;
            }
            $this->queryLog?->debug('FACETS STATS (literal facets) time ' . (microtime(true) - $t1));
            $t1 = microtime(true);
        }
        // RANGE FACETS
        foreach ($this->facets as $facet) {
            if ($facet->type !== self::FACET_CONTINUOUS) {
                continue;
            }
            $param = [
                $facet->min ?? null, $facet->max ?? null,
                $facet->max ?? null, $facet->min ?? null,
                $facet->property,
            ];

            if ($facet->precision === 0) {
                $stepExpr = "CASE WHEN range > least(?, nd) THEN range / least(?, nd) ELSE 1 END";
                $param    = array_merge($param, array_fill(0, 4, $facet->bins));
                $filter   = "OR step = 1";
            } else {
                $stepExpr = "range / least(?, nd)";
                $param    = array_merge($param, array_fill(0, 2, $facet->bins));
                $filter   = "";
            }
            $query  = "
                WITH
                    limits AS (
                        SELECT 
                            greatest(min(lower(value::numrange)), ?::numeric) AS start,
                            least(max(upper(value::numrange)), ?::numeric) AS stop,
                            least(max(upper(value::numrange)), ?::numeric) - greatest(min(lower(value::numrange)), ?::numeric) AS range,
                            greatest(count(DISTINCT value), 1) AS nd
                        FROM " . self::TEMPTABNAME . "
                        WHERE facet = ?
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
                    count(DISTINCT id) AS count,
                    lower(bin) AS lower,
                    upper(bin) AS upper
                FROM
                    bins b
                    JOIN " . self::TEMPTABNAME . " m ON facet = ? AND b.bin && value::numrange
                GROUP BY b.bin
                ORDER BY lower(bin)
            ";
            $param  = array_merge($param, [$facet->precision, $facet->property]);
            $query  = $this->pdo->prepare($query);
            $query->execute($param);
            $values = $query->fetchAll(PDO::FETCH_OBJ);
            if (count($values) > 0) {
                $stats[$facet->property]->values = $values;
                $stats[$facet->property]->min    = (float) reset($values)?->lower;
                $stats[$facet->property]->max    = (float) end($values)?->upper;
            }
            $this->queryLog?->debug('FACETS STATS (' . $facet->property . ') time ' . (microtime(true) - $t1));
            $t1 = microtime(true);
        }

        // MATCH PROPERTY
        if (isset($stats[self::FACET_MATCH])) {
            $query                            = $this->pdo->query("
                SELECT 
                    property AS value, 
                    property AS label, 
                    count(DISTINCT id) AS count 
                FROM " . self::TEMPTABNAME . "
                WHERE property IS NOT NULL
                GROUP BY 1 
                ORDER BY 2 DESC
            ");
            $stats[self::FACET_MATCH]->values = $query->fetchAll(PDO::FETCH_OBJ);
        }
        $this->queryLog?->debug('FACETS STATS (match property) time ' . (microtime(true) - $t1));
        $t1 = microtime(true);

        // LINK PROPERTY
        if ($this->linkNamedEntities()) {
            $query                           = $this->pdo->query("
                SELECT
                    link_property AS value,
                    link_property AS label, 
                    count(DISTINCT id) AS count
                FROM " . self::TEMPTABNAME . "
                WHERE link_property IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
            ");
            $stats[self::FACET_LINK]->values = $query->fetchAll(PDO::FETCH_OBJ);
        }
        $this->queryLog?->debug('FACETS STATS (link property) time ' . (microtime(true) - $t1));
        $t1 = microtime(true);

        // MAP
        if (isset($stats[self::FACET_MAP])) {
            $query                          = $this->pdo->query("
                SELECT st_asgeojson(st_union(st_centroid(geom::geometry)))
                FROM (
                    SELECT *
                    FROM spatial_search s JOIN metadata m USING (mid)
                    WHERE EXISTS (SELECT 1 FROM " . self::TEMPTABNAME . " WHERE id = m.id)
                  UNION
                    SELECT *
                    FROM spatial_search s JOIN metadata m USING (mid)
                    WHERE EXISTS (SELECT 1 FROM " . self::TEMPTABNAME . " JOIN relations r USING (id) WHERE r.target_id = m.id)
                ) t
            ");
            $stats[self::FACET_MAP]->values = $query->fetchColumn() ?: '';
        }
        $this->queryLog?->debug('FACETS STATS (map) time ' . (microtime(true) - $t1));

        $this->queryLog?->debug('FACETS STATS time ' . (microtime(true) - $t));
        return $this->postprocessFacets($stats, $prefLang);
    }

    public function closeSearch(): void {
        $this->pdo->rollBack();
    }

    public function getInitialFacets(string $prefLang, string $cacheFile = '',
                                     bool $force = false): array {
        $lastMod = $this->pdo->prepare("SELECT max(value_t) FROM metadata WHERE property = ?");
        $lastMod->execute([(string) $this->schema->modificationDate]);
        $lastMod = $lastMod->fetchColumn();

        $cache = (object) ['date' => '', 'facets' => []];
        if (file_exists($cacheFile)) {
            $cache = json_decode(file_get_contents($cacheFile));
        }

        if ($cache->date < $lastMod || $force) {
            $cache->date   = $lastMod;
            $cache->facets = [];

            $acceptedTypes = [
                self::FACET_OBJECT, self::FACET_LITERAL, self::FACET_CONTINUOUS,
                self::FACET_MAP
            ];
            foreach ($this->facets as $facet) {
                if (!in_array($facet->type, $acceptedTypes)) {
                    continue;
                }
                $out         = clone($facet);
                $out->values = match ($facet->type) {
                    self::FACET_OBJECT, self::FACET_LITERAL => $this->getInitialFacetDiscrete($facet, $prefLang),
                    self::FACET_MAP => $this->getInitialFacetMap(),
                    default => [],
                };
                if ($facet->type === self::FACET_CONTINUOUS) {
                    list($out->min, $out->max) = $this->getInitialFacetContinues($facet);
                }
                $cache->facets[] = $out;
            }

            if (!empty($cacheFile)) {
                file_put_contents($cacheFile, json_encode($cache, JSON_UNESCAPED_SLASHES));
            }
        }
        return $this->postprocessFacets($cache->facets, $prefLang);
    }

    private function getInitialFacetDiscrete(object $facet, string $prefLang): array {
        $param       = [];
        $weightQuery = '';
        $weightJoin  = '';
        $weightOrder = '';
        if (is_array($facet->weights)) {
            $tmpQuery    = $this->getWeightsWith($facet->weights, 'weight', $facet->type === 'object' ? 'bigint' : 'text');
            $weightQuery = "WITH w " . $tmpQuery->query;
            $weightJoin  = ($facet->type === 'object' ? 'LEFT ' : '') . "JOIN w USING (value)";
            $weightOrder = "weight DESC NULLS LAST,";
            $param       = array_merge($param, $tmpQuery->param);
        }
        if ($facet->type === 'object') {
            $query   = "
                $weightQuery
                SELECT ? || value::text AS value, label, count
                FROM 
                    (
                        SELECT DISTINCT ON (id)
                            id AS value,
                            value AS label,
                            count
                        FROM
                            (
                                SELECT target_id AS id, count(*) AS count
                                FROM relations
                                WHERE property = ?
                                GROUP BY 1
                            ) r
                            JOIN metadata m USING (id)
                        WHERE property = ?
                        ORDER BY id, lang = ? DESC
                    ) t
                    $weightJoin
                ORDER BY $weightOrder count DESC, label
            ";
            $param[] = $this->repo->getBaseUrl();
            $param[] = $facet->property;
            $param[] = (string) $this->schema->label;
            $param[] = $prefLang;
        } else {
            $query   = "
                $weightQuery
                SELECT value, value AS label, count
                FROM
                    (
                        SELECT value, count(*) AS count
                        FROM (
                            SELECT DISTINCT ON (id) id, value
                            FROM metadata
                            WHERE property = ?
                            ORDER BY id, lang = ? DESC
                        ) t
                        GROUP BY 1
                    ) c
                    $weightJoin
                ORDER BY $weightOrder count DESC
            ";
            $param[] = $facet->property;
            $param[] = $prefLang;
        }
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        return $query->fetchAll(PDO::FETCH_OBJ);
    }

    private function getInitialFacetMap(): string {
        $query = $this->pdo->query("
            SELECT st_asgeojson(st_union(st_centroid(geom::geometry)))
            FROM spatial_search 
            WHERE id IS NULL
        ");
        return $query->fetchColumn();
    }

    private function getInitialFacetContinues(object $facet): array {
        $plch  = substr(str_repeat(', ?', count($facet->start)), 2);
        $query = $this->pdo->prepare("SELECT min(value_n) FROM metadata WHERE property IN ($plch)");
        $query->execute($facet->start);
        $min   = $query->fetchColumn();

        $plch  = substr(str_repeat(', ?', count($facet->end)), 2);
        $query = $this->pdo->prepare("SELECT max(value_n) FROM metadata WHERE property IN ($plch)");
        $query->execute($facet->end);
        $max   = $query->fetchColumn();

        return [(int) $min, (int) $max];
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
        if (count($weights) === 0) {
            throw new RepoLibException('Empty weights list');
        }
        foreach ($weights as $k => $v) {
            $query->query   .= "(?::$valueType, ?::float),";
            $query->param[] = $k;
            $query->param[] = $v;
        }
        $query->query = substr($query->query, 0, -1) . ")";
        return $query;
    }

    private function linkNamedEntities(): bool {
        return count($this->linkFacet->classes) > 0;
    }

    private function postprocessFacets(array $facets, string $prefLang): array {
        foreach ($facets as $i) {
            $i->label = (array) $i->label;
            $i->label = $i->label[$prefLang] ?? reset($i->label);
            unset($i->defaultWeight);
            unset($i->weights);
        }
        return $facets;
    }
}
