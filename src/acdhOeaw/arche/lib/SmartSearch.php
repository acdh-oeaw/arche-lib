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
    private array $propWeights              = [];
    private float $propDefaultWeight        = 1.0;
    private array $facets                   = [];
    private float $facetsDefaultWeight      = 1.0;
    private float $exactWeight              = 10.0;
    private float $langWeight               = 10.0;
    private string $namedEntitiesProperty    = RDF::RDF_TYPE;
    private array $namedEntitiesValues      = [];
    private array $namedEntityWeights       = [];
    private float $namedEntityDefaultWeight = 1.0;
    private string $phrase;
    private string $oderExp                  = '';
    private ?AbstractLogger $queryLog;

    public function __construct(PDO $pdo, Schema $schema) {
        $this->pdo    = $pdo;
        $this->repo   = new RepoDb('', $schema, new Schema(new \stdClass()), $this->pdo);
        $this->schema = $schema;
        $this->facets = [
            $schema->modificationDate => 1.0,
        ];
    }

    public function setPropertyWeights(array $weights,
                                       float $defaultWeight = 1.0): self {
        $this->propWeights       = $weights;
        $this->propDefaultWeight = $defaultWeight;
        return $this;
    }

    /**
     * Results with a matching weight are 
     * @param array<string, string|array<string, float>> $properties
     * @return self
     */
    public function setFacetWeights(array $facets, float $defaultWeight = 1.0): self {
        $this->facets              = $facets;
        $this->facetsDeafultWeight = $defaultWeight;
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

    public function setNamedEntityFilter(array $values,
                                         string $property = RDF::RDF_TYPE): self {
        $this->namedEntitiesValues   = $values;
        $this->namedEntitiesProperty = $property;
        return $this;
    }

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
     * @param bool $linkNamedEntities
     * @param array<SearchTerm> $searchTerms
     * @return void
     */
    public function search(string $phrase, string $language = '',
                           bool $inBinary = true, array $allowedProperties = [],
                           array $searchTerms = []): void {
        $linkNamedEntities = count($this->namedEntitiesValues) > 0;
        $this->phrase      = $phrase;
        $query             = "WITH\n";
        $param             = [];

        // FILTERS
        $filters = '';
        if (count($searchTerms) > 0) {
            $filters = "AND EXISTS (SELECT 1 FROM filters WHERE id = s.id)";
            $baseUrl = $this->repo->getBaseUrl();
            $idProp  = $this->repo->getSchema()->id;
            $query   .= "filters AS (\nSELECT id FROM\n";
            foreach ($searchTerms as $n => $st) {
                /* @var $st SearchTerm */
                $qp    = $st->getSqlQuery($baseUrl, $idProp, []);
                $query .= $n > 0 ? "JOIN (" : "(";
                $query .= $qp->query;
                $query .= $n > 0 ? ") f$n USING (id)\n" : ") f$n\n";
                $param = array_merge($param, $qp->param);
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
        $n = '0';
        foreach ($this->facets as $prop => $weights) {
            $facetsFrom        .= "LEFT JOIN metadata meta_$n ON s.id = meta_$n.id AND meta_$n.property = ?\n";
            $facetsFromParam[] = $prop;
            if (!is_array($weights)) {
                $facetsSelect .= ", CASE WHEN meta_$n.value_t IS NOT NULL THEN to_char(meta_$n.value_t, 'YYYY-MM-DD') ELSE meta_$n.value END AS meta_$n";
                $desc         = strtolower($weights) === 'desc';
                $orderExp     .= ", " . ($desc ? "max" : "min") . "(meta_$n)" . ($desc ? " DESC" : "") . " NULLS LAST";
            } elseif (count($weights) > 0) {
                $facetsFrom       .= "LEFT JOIN weights_$n ON meta_$n.value = weights_$n.value\n";
                $facetsSelect     .= ", meta_$n.value AS meta_$n, weight_$n";
                $orderExp         .= ", min(weight_$n) NULLS LAST";
                $weightExp        .= " * coalesce(weight_$n, ?)";
                $weightExpParam[] = $this->facetsDefaultWeight;

                $queryTmp = $this->getWeightsWith($weights, "weight_$n");
                $query    .= "weights_$n $queryTmp->query,\n";
                $param    = array_merge($param, $queryTmp->param);
            } else {
                $facetsSelect .= ", meta_$n.value AS meta_$n";
            }
            $n = (string) (((int) $n) + 1);
        }

        // INITIAL SEARCH
        $langMatch   = !empty($language) ? "sm.lang = ?" : "?::bool";
        $langParam   = !empty($language) ? $language : 0.0;
        $inBinary    = $inBinary ? "" : "AND f.id IS NULL";
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
        $query  .= "
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
                    $inBinary
                    $propsFilter
            )
        ";
        $param  = array_merge(
            $param,
            [
                $this->schema->id,
                $phrase, $this->exactWeight, $langParam, $this->langWeight,
                SearchTerm::escapeFts($phrase)
            ],
                                      $propsParam
        );
        $curTab = 'search1';

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
                                JOIN weights_p w ON s.property = w.value 
                            ORDER BY id, weight_p * weight_m DESC
                        ) s
                        JOIN metadata mne ON s.id = mne.id AND mne.property = ? AND mne.value IN ($neIn)
                        JOIN relations r ON s.id = r.target_id
                )
            ";
            $param            = array_merge(
                $param,
                [$this->namedEntitiesProperty],
                $this->namedEntitiesValues
            );
            $weightExp        .= " * coalesce(weight_ne, ?)";
            $weightExpParam[] = $this->namedEntityDefaultWeight;
            $facetsFrom       .= "LEFT JOIN weights_ne wne ON s.link_property = wne.value\n";

            $queryTmp = $this->getWeightsWith($this->namedEntityWeights, 'weight_ne');
            $query    .= ", weights_ne $queryTmp->query\n";
            $param    = array_merge($param, $queryTmp->param);
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

        //exit(new QueryPart($query, $param));
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
        $config->metadataMode             = 'resource';

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

    public function getSearchFacets(): array {
        $stats = [];

        $stats['property'] = [
            'continues' => false,
            'values'    => []
        ];
        $query             = $this->pdo->query("
            SELECT property AS value, count(DISTINCT id) AS count 
            FROM " . self::TEMPTABNAME . "
            GROUP BY 1 
            ORDER BY 2 DESC
        ");
        while ($row               = $query->fetchObject()) {
            $stats['property']['values'][$row->value] = $row->count;
        }

        if ($this->linkNamedEntities()) {
            $query                 = $this->pdo->query("
                SELECT link_property AS value, count(DISTINCT id) AS count
                FROM " . self::TEMPTABNAME . "
                WHERE link_property IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
            ");
            $stats['linkProperty'] = [
                'continues' => false,
                'values'    => []
            ];
            while ($row                   = $query->fetchObject()) {
                $stats['linkProperty']['values'][$row->value] = $row->count;
            }
        }

        foreach (array_keys($this->facets) as $n => $key) {
            $query       = $this->pdo->query("
                SELECT meta_$n AS value, count(DISTINCT id) AS count 
                FROM " . self::TEMPTABNAME . "
                WHERE meta_$n IS NOT NULL
                GROUP BY 1 
                ORDER BY 2 DESC
            ");
            $stats[$key] = [
                'values'    => [],
                'continues' => !is_array($this->facets[$key]),
            ];
            while ($row         = $query->fetchObject()) {
                $value                         = is_numeric($row->value) ? (float) $row->value : $row->value;
                $stats[$key]['values'][$value] = $row->count;
            }
        }

        return $stats;
    }

    public function closeSearch(): void {
        $this->pdo->rollBack();
    }

    private function getWeightsWith(array $weights, string $weightName): QueryPart {
        $query = new QueryPart("(value, $weightName) AS (VALUES ");
        foreach ($weights as $k => $v) {
            $query->query   .= "(?::text, ?::float),";
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
