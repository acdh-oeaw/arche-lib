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
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Provides an API for advanced weighted 
 *
 * @author zozlak
 */
class SmartSearch {

    const TAB_MATCHES      = "_matches";
    const TAB_FILTERS      = "_filters";
    const TAB_SEARCH       = "_search";
    const TAB_PARENTS      = '_parents';
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
    private array $facets             = [];
    private object $matchFacet;
    private object $linkFacet;
    private float $exactWeight        = 2.0;
    private float $langWeight         = 1.5;
    private string $phrase;
    private SearchTerm $spatialTerm;
    private ?AbstractLogger $queryLog = null;

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
     */
    public function search(string $phrase, string $language = '',
                           bool $inBinary = true, array $allowedProperties = [],
                           array $searchTerms = [],
                           ?SearchTerm $spatialTerm = null,
                           array $parentIds = [], int $matchesLimit = 10000): bool {
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        $this->phrase = $phrase;
        if ($spatialTerm !== null) {
            $this->spatialTerm = $spatialTerm;
        } else {
            unset($this->spatialTerm);
        }

        // INITIAL SEARCH
        $filteredSearch = $this->createFiltersTable($searchTerms, $parentIds);
        $queries        = [];
        if (!empty($phrase)) {
            $queries[] = $this->getFtsQuery($phrase, $allowedProperties, $inBinary, $language);
        }
        if ($spatialTerm !== null) {
            $queries[] = $this->getSpatialQuery($spatialTerm, $allowedProperties);
        }
        if (count($queries) === 0) {
            if (!$filteredSearch) {
                $this->pdo->query("
                    CREATE TEMPORARY TABLE " . self::TAB_MATCHES . " AS (
                        SELECT null::bigint AS id, null::bigint AS ftsid, null::text AS property, null::text AS link_property, null::text AS facet, null::text AS value, null::float AS weight 
                        WHERE false
                    )");
                return false; // an emtpy search
            }
            $query = $this->getFiltersQuery(); // no phrase nor spatial term - fallback to searchTerms
        } else {
            $query = $this->combineSearchQueries($queries, $filteredSearch, self::TAB_SEARCH);
        }
        $query->execute($this->pdo);

        // MATCHES INCLUDING FACETS
        $query = new QueryPart("WITH\n", log: $this->queryLog);
        foreach ($this->facets as $mn => $facet) {
            if (is_array($facet->weights) && !($facet === $this->matchFacet || $facet === $this->linkFacet)) {
                $tmpQuery     = $this->getWeightsWith($facet->weights, "weight_$mn", $facet->type === 'object' ? 'bigint' : 'text');
                $query->query .= "weights_$mn $tmpQuery->query,\n";
                $query->param = array_merge($query->param, $tmpQuery->param);
            }
        }
        $query->query = count($query->param) === 0 ? '' : substr($query->query, 0, -2) . "\n";
        $query->query .= "SELECT * FROM " . self::TAB_SEARCH . "\n";
        $this->addFacetMatches($query);
        $query->query = "CREATE TEMPORARY TABLE " . self::TAB_MATCHES . " AS\n$query->query";
        $query->execute($this->pdo);

        // CLEANUP
        $query = "DELETE FROM " . self::TAB_MATCHES . " WHERE id IN (SELECT id FROM " . self::TAB_MATCHES . " WHERE weight = 0)";
        $query = new QueryPart($query, log: $this->queryLog);
        $query->execute($this->pdo);

        return true;
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
        $oGroupBy = $oOrderBy = $oQuery   = '';
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
                        AND EXISTS (SELECT 1 FROM " . self::TAB_MATCHES . " WHERE id = m.id)
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
                FROM " . self::TAB_MATCHES . "
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
                    JOIN " . self::TAB_MATCHES . " USING (id)
                    JOIN full_text_search USING (ftsid)
              UNION
                -- match boolean properties - here store property with the match
                SELECT id, ? AS property, ? AS type, '' AS lang, coalesce(link_property, property) AS value, ftsid
                FROM
                    _page
                    JOIN " . self::TAB_MATCHES . " USING (id)
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
     * @return array<string, object>
     */
    public function getSearchFacets(string $prefLang = ''): array {
        $stats = [];
        $t     = microtime(true);
        $t1    = $t;

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
                           FROM " . self::TAB_MATCHES . " s
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
                FROM " . self::TAB_MATCHES . "
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
                        FROM " . self::TAB_MATCHES . "
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
                    JOIN " . self::TAB_MATCHES . " m ON facet = ? AND b.bin && value::numrange
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
                FROM " . self::TAB_MATCHES . "
                WHERE property IS NOT NULL
                GROUP BY 1 
                ORDER BY 2 DESC
            ");
            $stats[self::FACET_MATCH]->values = $query->fetchAll(PDO::FETCH_OBJ);
        }
        $this->queryLog?->debug('FACETS STATS (match property) time ' . (microtime(true) - $t1));
        $t1 = microtime(true);

        // LINK PROPERTY
        if (isset($stats[self::FACET_LINK])) {
            $query                           = $this->pdo->query("
                SELECT
                    link_property AS value,
                    link_property AS label, 
                    count(DISTINCT id) AS count
                FROM " . self::TAB_MATCHES . "
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
            if (isset($this->spatialTerm)) {
                $query = $this->pdo->prepare("
                SELECT st_asgeojson(st_union(DISTINCT st_centroid((value::geography)::geometry)))
                FROM " . self::TAB_MATCHES . "
                WHERE facet = ?
            ");
                $query->execute([self::FACET_MAP]);
            } else {
                $query = $this->pdo->query("
                SELECT st_asgeojson(st_union(st_centroid(geom::geometry)))
                FROM (
                    SELECT *
                    FROM spatial_search s JOIN metadata m USING (mid)
                    WHERE EXISTS (SELECT 1 FROM " . self::TAB_MATCHES . " WHERE id = m.id)
                  UNION
                    SELECT *
                    FROM spatial_search s JOIN metadata m USING (mid)
                    WHERE EXISTS (SELECT 1 FROM " . self::TAB_MATCHES . " JOIN relations r USING (id) WHERE r.target_id = m.id)
                ) t
            ");
            }
            $stats[self::FACET_MAP]->values = $query->fetchColumn() ?: '';
        }
        $this->queryLog?->debug('FACETS STATS (map) time ' . (microtime(true) - $t1));

        $this->queryLog?->debug('FACETS STATS time ' . (microtime(true) - $t));
        return $this->postprocessFacets($stats, $prefLang);
    }

    public function closeSearch(): void {
        $this->pdo->rollBack();
    }

    /**
     * 
     * @return array<object>
     */
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

    /**
     * 
     * @return array<object>
     */
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
        ");
        return $query->fetchColumn();
    }

    /**
     * 
     * @return array{0: int, 1: int}
     */
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

    /**
     * @param array<string, object> $facets
     * @return array<string, object>
     */
    private function postprocessFacets(array $facets, string $prefLang): array {
        foreach ($facets as $i) {
            $i->label = (array) $i->label;
            $i->label = $i->label[$prefLang] ?? reset($i->label);
            unset($i->defaultWeight);
            unset($i->weights);
        }
        return $facets;
    }

    /**
     * 
     * @param array<SearchTerm> $searchTerms
     * @param array<int> $parentIds
     */
    private function createFiltersTable(array $searchTerms, array $parentIds): bool {
        if (count($searchTerms) + count($parentIds) === 0) {
            return false;
        }

        $baseUrl     = $this->repo->getBaseUrl();
        $filterQuery = "CREATE TEMPORARY TABLE " . self::TAB_FILTERS . " AS (\nSELECT id FROM\n";
        $filterParam = [];

        $n = 0;
        if (count($parentIds) > 0) {
            $tab          = count($searchTerms) > 0 ? self::TAB_PARENTS : self::TAB_FILTERS;
            $parentsQuery = "CREATE TEMPORARY TABLE $tab AS (\n";
            $parentsParam = [];
            foreach ($parentIds as $m => $id) {
                $parentsQuery .= $m > 0 ? "UNION\n" : "";
                $parentsQuery .= "SELECT id FROM get_relatives(?::bigint, ?, 999999, 0, false, false) WHERE n <> 0\n";
                $parentsParam = array_merge($parentsParam, [$id, $this->schema->parent]);
            }
            $parentsQuery .= ")";
            $query        = new QueryPart($parentsQuery, $parentsParam, log: $this->queryLog);
            $query->execute($this->pdo);

            $filterQuery .= self::TAB_PARENTS . "\n";
            $n           = 1;
        }

        if (count($searchTerms) > 0) {
            foreach ($searchTerms as $st) {
                /* @var $st SearchTerm */
                $tmpQuery    = $st->getSqlQuery($baseUrl, $this->schema->id, []);
                $filterQuery .= $n > 0 ? "JOIN (" : "(";
                $filterQuery .= $tmpQuery->query;
                $filterQuery .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
                $filterParam = array_merge($filterParam, $tmpQuery->param);
                $n++;
            }
            $filterQuery .= ")\n";
            $query       = new QueryPart($filterQuery, $filterParam, log: $this->queryLog);
            $query->execute($this->pdo);
        }
        return true;
    }

    /**
     * 
     * @param array<string> $allowedProperties
     */
    private function getFtsQuery(string $phrase, array $allowedProperties,
                                 bool $inBinary, string $lang): QueryPart {
        $langMatch = !empty($lang) ? "sm.lang = ?" : "?::bool";
        $langParam = !empty($lang) ? $lang : 0.0;
        $query     = "
            SELECT
                coalesce(f.id, f.iid, sm.id) AS id,
                ftsid,
                CASE
                    WHEN sm.property IS NOT NULL THEN sm.property
                    WHEN f.iid IS NOT NULL THEN ?
                    ELSE 'BINARY'
                END AS property,
                null::text AS link_property,
                null::text AS facet,
                null::text AS value,
                CASE WHEN raw ILIKE ? THEN ? ELSE 1.0 END * CASE WHEN $langMatch THEN ? ELSE 1.0 END AS weight
            FROM
                full_text_search f
                LEFT JOIN metadata sm using (mid)
            WHERE
                (websearch_to_tsquery('simple', ?) @@ segments OR raw ILIKE ?)
        ";
        $param     = [
            $this->schema->id,
            $phrase, $this->exactWeight, $langParam, $this->langWeight,
            SearchTerm::escapeFts($phrase), "%$phrase%",
        ];
        if (!$inBinary) {
            $query .= "AND f.id IS NULL\n";
        }
        if (count($allowedProperties) > 0) {
            $query .= "AND CASE 
                WHEN sm.property IS NOT NULL THEN sm.property 
                WHEN f.iid IS NOT NULL THEN ? 
                ELSE 'BINARY' 
            END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
            $param = array_merge(
                $param,
                [$this->schema->id],
                $allowedProperties
            );
        }
        return new QueryPart($query, $param);
    }

    /**
     * 
     * @param array<string> $allowedProperties
     */
    private function getSpatialQuery(SearchTerm $term, array $allowedProperties): QueryPart {
        $baseUrl      = $this->repo->getBaseUrl();
        $query        = $term->getSqlQuery($baseUrl, $this->schema->id, []);
        $query->query = (string) preg_replace('/^.*FROM/sm', '', $query->query);
        $query->query = "
            SELECT
                coalesce(ss.id, m.id) AS id,
                -1::bigint AS ftsid,
                CASE
                    WHEN m.property IS NOT NULL THEN m.property
                    ELSE 'GEODATA'
                END AS property,
                null::text AS link_property,
                ?::text AS facet,
                geom::text AS value,
                1.0 AS weight
            FROM $query->query
        ";
        array_unshift($query->param, self::FACET_MAP);
        if (count($allowedProperties) > 0) {
            $query->query .= "AND CASE 
                WHEN m.property IS NOT NULL THEN m.property 
                ELSE 'GEODATA' 
            END IN (" . substr(str_repeat(', ?', count($allowedProperties)), 2) . ")";
            $query->param = array_merge(
                $query->param,
                $allowedProperties
            );
        }
        return $query;
    }

    private function getFiltersQuery(): QueryPart {
        return new QueryPart("
            CREATE TEMPORARY TABLE " . self::TAB_SEARCH . " AS
            SELECT 
                id, 
                -1::bigint AS ftsid,
                null::text AS property,
                null::text AS link_property,
                null::text AS facet,
                null::text AS value,
                1.0 AS weight
            FROM " . self::TAB_FILTERS . "
        ");
    }

    /**
     * 
     * @param array<QueryPart> $queries
     * @param bool $filteredSearch
     * @param string $outName
     * @return QueryPart
     */
    private function combineSearchQueries(array $queries, bool $filteredSearch,
                                          string $outName): QueryPart {
        $filterExpExists = $filteredSearch ? " WHERE EXISTS (SELECT 1 FROM " . self::TAB_FILTERS . " WHERE id = search.id)" : "";
        $filterExpJoin   = $filteredSearch ? " JOIN " . self::TAB_FILTERS . " USING (id)" : "";

        $query = new QueryPart("CREATE TEMPORARY TABLE $outName AS\nWITH\n", log: $this->queryLog);
        if (count($this->matchFacet->weights) > 0) {
            $tmpQuery     = $this->getWeightsWith($this->matchFacet->weights, 'weight_p');
            $query->query .= "    weights_p $tmpQuery->query,\n";
            $query->param = array_merge($query->param, $tmpQuery->param);
        }
        if (count($this->linkFacet->weights) > 0) {
            $tmpQuery     = $this->getWeightsWith($this->linkFacet->weights, 'weight_l');
            $query->query .= "    weights_l $tmpQuery->query,\n";
            $query->param = array_merge($query->param, $tmpQuery->param);
        }

        foreach ($queries as $n => $i) {
            $i = $this->linkNamedEntities($i, 'search', $filterExpJoin);
            //$i->query .= $filterExpExists;
        }

        if (count($queries) === 1) {
            $tmpQuery     = reset($queries);
            $query->query = substr($query->query, 0, -2) . "\nSELECT * FROM (" . $tmpQuery->query . ") $outName";
            $query->param = array_merge($query->param, $tmpQuery->param);
        } else {
            foreach ($queries as $n => $tmpQuery) {
                $query->query .= "    _t{$n} AS (" . $tmpQuery->query . "),\n";
                $query->param = array_merge($query->param, $tmpQuery->param);
            }
            $query->query = substr($query->query, 0, -2);
            $query->query .= "
                SELECT * FROM (
                    SELECT * FROM _t0 WHERE EXISTS (SELECT 1 FROM _t1 WHERE _t0.id = id)
                  UNION
                    SELECT * FROM _t1 WHERE EXISTS (SELECT 1 FROM _t0 WHERE _t1.id = id)
                ) $outName";
        }
        return $query;
    }

    private function linkNamedEntities(QueryPart $query, string $outName,
                                       string $joinQuery = ''): QueryPart {
        if (count($this->linkFacet->classes) === 0) {
            $query->query = "SELECT * FROM ($query->query) $outName $joinQuery";
            return $query;
        }

        $neIn         = substr(str_repeat('?, ', count($this->linkFacet->classes)), 0, -2);
        $query->query = "WITH s AS (" . $query->query . ")";
        $query->query .= "
            SELECT * FROM (
                SELECT 
                    id, ftsid, property, link_property, facet, s.value,
                    weight * coalesce(weight_p, ?) AS weight
                FROM
                    s
                    $joinQuery
                    LEFT JOIN weights_p w ON s.property = w.value
              UNION
                SELECT 
                    r.id, 
                    t.ftsid, 
                    t.property, 
                    r.property AS link_property, 
                    t.facet,
                    t.value,
                    t.weight * coalesce(t.weight_p, ?) * coalesce(wl.weight_l, ?) AS weight
                FROM
                    (
                        SELECT DISTINCT ON (id) s.*, w.weight_p 
                        FROM 
                            s
                            $joinQuery
                            LEFT JOIN weights_p w ON s.property = w.value 
                        ORDER BY id, coalesce(weight_p, ?) * weight DESC
                    ) t
                    JOIN metadata mne ON t.id = mne.id AND mne.property = ? AND mne.value IN ($neIn)
                    JOIN relations r ON t.id = r.target_id
                    LEFT JOIN weights_l wl ON r.property = wl.value
            ) $outName
        ";
        $query->param = array_merge(
            $query->param,
            [
                $this->matchFacet->defaultWeight, // first union part
                $this->matchFacet->defaultWeight, // select of second union part
                $this->linkFacet->defaultWeight, // select of second union part
                $this->matchFacet->defaultWeight, // subselect of second union part
                $this->linkFacet->property // join with mne
            ],
            $this->linkFacet->classes, // join with mne
        );
        return $query;
    }

    private function addFacetMatches(QueryPart $query): void {
        $filterQueryExists = "AND EXISTS (SELECT 1 FROM " . self::TAB_SEARCH . " WHERE id = m.id)";
        $filterQueryJoin   = "JOIN " . self::TAB_SEARCH . " USING (id)";

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
                $weightQuery    = "LEFT JOIN weights_$mn w ON m.$valCol = w.value";
                $weightValue    = "coalesce(w.weight_$mn, ?)";
                $query->param[] = $facet->defaultWeight;
            }
            $query->query   .= "UNION
                SELECT
                    m.id, 
                    null::bigint as fstid, 
                    null::text AS property, 
                    null::text AS link_property, 
                    m.property AS facet, 
                    m.$valCol::text AS value,
                    $weightValue AS weight
                FROM 
                    $srcTab m
                    $filterQueryJoin
                    $weightQuery
                WHERE
                    m.property = ?
            "; // . $filterQueryExists;
            $query->param[] = $facet->property;
        }
        // CONTINUOUS FACETS DATA
        foreach ($this->facets as $facet) {
            if ($facet->type !== self::FACET_CONTINUOUS) {
                continue;
            }
            $minPlch      = substr(str_repeat(', ?', count($facet->start)), 2);
            $maxPlch      = substr(str_repeat(', ?', count($facet->end)), 2);
            $query->query .= "UNION
                SELECT
                    t1.id,
                    null::bigint AS ftsid,
                    null::text AS property,
                    null::text AS link_property,
                    ?::text AS facet, 
                    '[' || vmin::text || ', ' || vmax::text || ']' AS value,
                    null::float AS weight
                FROM
                    (
                        SELECT id, min(m.value_n) AS vmin
                        FROM metadata m
                        WHERE
                            m.property IN ($minPlch)
                            $filterQueryExists
                        GROUP BY 1
                    ) t1 
                    JOIN (
                        SELECT id, max(m.value_n) AS vmax
                        FROM metadata m
                        WHERE
                            m.property IN ($maxPlch)
                            $filterQueryExists
                        GROUP BY 1
                    ) t2 USING (id)
            ";
            $query->param = array_merge($query->param, [$facet->property], $facet->start, $facet->end);
        }
    }
}
