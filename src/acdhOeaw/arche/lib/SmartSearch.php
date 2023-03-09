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
    private array $facets                   = [];
    private float $exactWeight              = 10.0;
    private float $langWeight               = 10.0;
    private string $namedEntitiesProperty    = RDF::RDF_TYPE;
    private array $namedEntitiesValues      = [];
    private array $namedEntityWeights       = [];
    private float $namedEntityDefaultWeight = 1.0;
    private string $phrase;
    private ?AbstractLogger $queryLog;

    public function __construct(PDO $pdo, Schema $schema) {
        $this->pdo    = $pdo;
        $this->repo   = new RepoDb('', $schema, new Schema(new \stdClass()), $this->pdo);
        $this->schema = $schema;
        $this->facets = [
            $schema->modificationDate => 1.0,
        ];
    }

    public function setPropertyWeights(array $weights): self {
        $this->propWeights = $weights;
        return $this;
    }

    /**
     * Results with a matching weight are 
     * @param array<string, string|array<string, float>> $properties
     * @return self
     */
    public function setFacetWeights(array $facets): self {
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

    public function search(string $phrase, string $language = '',
                           bool $inBinary = true, bool $linkNamedEntities = true): void {
        $this->phrase = $phrase;
        $query        = "WITH\n";
        $param        = [];

        // WEIGHTS
        $queryTmp = $this->getWeightsWith($this->propWeights, 'weight_p');
        $query    .= "weights_p $queryTmp->query,\n";
        $param    = array_merge($param, $queryTmp->param);

        if ($linkNamedEntities && count($this->namedEntitiesValues) > 0) {
            $queryTmp = $this->getWeightsWith($this->namedEntityWeights, 'weight_ne');
            $query    .= "weights_ne $queryTmp->query,\n";
            $param    = array_merge($param, $queryTmp->param);
        }

        $n              = '0';
        $searchSelect   = "";
        $searchFrom     = "";
        $weightExpr     = "CASE WHEN exact_match THEN ? ELSE 1.0 END * CASE WHEN lang_match THEN ? ELSE 1.0 END * coalesce(weight_p, 1.0)";
        $weightedSelect = "";
        $weightedFrom   = "";
        $weightedOrder  = "";
        $searchParam    = [];
        foreach ($this->facets as $prop => $weights) {
            $searchFrom     .= "JOIN metadata meta_$n ON coalesce(f.id, f.iid, sm.id) = meta_$n.id AND meta_$n.property = ?\n";
            $searchParam[]  = $prop;
            $weightedSelect .= ", meta_$n";
            if (is_array($weights)) {
                $queryTmp       = $this->getWeightsWith($weights, "weight_$n");
                $query          .= "weights_$n $queryTmp->query,\n";
                $param          = array_merge($param, $queryTmp->param);
                $searchSelect   .= ", meta_$n.value AS meta_$n";
                $weightExpr     .= " * coalesce(weight_$n, 1.0)";
                $weightedFrom   .= "LEFT JOIN weights_$n ON s.meta_$n = weights_$n.value\n";
                $weightedOrder  .= ", weight_$n NULLS LAST";
                $weightedSelect .= ", weight_$n";
            } else {
                $searchSelect  .= ", CASE WHEN meta_$n.value_t IS NOT NULL THEN to_char(meta_$n.value_t, 'YYYY-MM-DD') ELSE meta_$n.value END AS meta_$n";
                $weightedOrder .= ", meta_$n" . (strtolower($weights) === 'desc' ? " DESC" : "") . " NULLS LAST";
            }
            $n = (string) (((int) $n) + 1);
        }

        // INITIAL SEARCH
        $langMatch = !empty($language) ? "sm.lang = ?" : "?";
        $language  = empty($language) ? 0 : $language;
        $inBinary  = $inBinary ? "" : "AND f.id IS NULL";
        $query     = "$query
            search AS (
                SELECT
                    coalesce(f.id, f.iid, sm.id) AS id,
                    ftsid,
                    CASE
                        WHEN sm.property IS NOT NULL THEN sm.property
                        WHEN f.iid IS NOT NULL THEN ?
                        ELSE 'BINARY'
                    END AS property,
                    NULL::bigint AS nn_id,
                    NULL::text AS property_nn,
                    raw = ? AS exact_match,
                    $langMatch AS lang_match
                    $searchSelect
                FROM
                    full_text_search f
                    LEFT JOIN metadata sm using (mid)
                    $searchFrom
                    $nnFrom                    
                WHERE
                    websearch_to_tsquery('simple', ?) @@ segments
                    $inBinary
            )
        ";
        $param     = array_merge(
            $param,
            [$this->schema->id, $phrase, $language],
            $searchParam,
            [SearchTerm::escapeFts($phrase)],
        );
        $curTab    = 'search';

        // WEIGHTS
        $query  = "$query,
            weighted AS (
                SELECT DISTINCT ON (id)
                    id, ftsid, property,
                    $weightExpr AS weight
                    $weightedSelect
                FROM
                    search s
                    LEFT JOIN weights_p ON weights_p.value = s.property
                    $weightedFrom
                ORDER BY id, weight DESC $weightedOrder
            )
        ";
        $param  = array_merge(
            $param,
            [$this->exactWeight, $this->langWeight]
        );
        $curTab = 'weighted';

        // NAMED ENTITIES
        if ($linkNamedEntities && count($this->namedEntitiesValues) > 0) {
            $neIn   = substr(str_repeat('?, ', count($this->namedEntitiesValues)), 0, -2);
            $query  = "$query,
                weighted2 AS (
                    SELECT * FROM weighted
                  UNION
                    SELECT DISTINCT ON (r.id)
                        r.id, w.ftsid, r.property, w.weight * coalesce(ww.weight_ne, ?::float) AS weight
                        $weightedSelect
                    FROM
                        weighted w
                        JOIN metadata mne ON w.id = mne.id AND mne.property = ? AND mne.value IN ($neIn)
                        JOIN relations r ON w.id = r.target_id
                        LEFT JOIN weights_ne ww ON r.property = ww.value
                    ORDER BY id, weight DESC
                ),
                weighted3 AS (
                    SELECT DISTINCT ON (id) *
                    FROM weighted2
                    ORDER BY id, weight DESC $weightedOrder
                )
            ";
            $param  = array_merge(
                $param,
                [$this->namedEntityDefaultWeight, $this->namedEntitiesProperty],
                $this->namedEntitiesValues,
            );
            $curTab = 'weighted3';
        }

        $query = "CREATE TEMPORARY TABLE " . self::TEMPTABNAME . " AS ($query SELECT * FROM $curTab ORDER BY weight DESC $weightedOrder)";
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        if (isset($this->queryLog)) {
            $this->queryLog->debug((string) (new QueryPart($query, $param)));
        }
        $this->pdo->beginTransaction();
        $query = $this->pdo->prepare($query);
        $query->execute($param);
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
        $offset                           = $page * $pageSize;

        // smart search metadata
        $phraseEsc = SearchTerm::escapeFts($this->phrase);
        $query     = "
          WITH page AS (SELECT * FROM " . self::TEMPTABNAME . " OFFSET ? LIMIT ?)
            SELECT null::bigint AS id, ? AS property, ? AS type, '' AS lang, count(*)::text AS value
            FROM page
          UNION
            SELECT id, ? AS property, ? AS type, '' AS lang, (row_number() OVER ())::text AS value
            FROM page
          UNION
            SELECT
                p.id, ? AS property, ? AS type, '' AS lang, ts_headline('simple', raw, websearch_to_tsquery('simple', ?), ?) AS value 
            FROM 
                page p
                JOIN full_text_search USING (ftsid)
          UNION
            SELECT id, ? AS property, ? AS type, '' AS lang, property AS value
            FROM page
          UNION
            SELECT id, ? AS property, ? AS type, '' AS lang, weight::text AS value
            FROM page
        ";
        $param = [
            $offset, $pageSize,
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
        while ($row       = $query->fetchObject()) {
            yield $row;
        }

        // metadata of matched resources
        $query = "
            SELECT *
            FROM " . self::TEMPTABNAME . "
            OFFSET ?
            LIMIT ?
        ";
        $param = [$offset, $pageSize];
        $query = $this->repo->getPdoStatementBySqlQuery($query, $param, $config);
        while ($row   = $query->fetchObject()) {
            yield $row;
        }
    }

    public function getSearchFacets(): array {
        $stats = [];
        $query = $this->pdo->query("
            SELECT property AS value, count(*) AS count 
            FROM " . self::TEMPTABNAME . "
            GROUP BY 1 
            ORDER BY 2 DESC
        ");
        while ($row   = $query->fetchObject()) {
            $stats['property'][$row->value] = $row->count;
        }

        foreach (array_keys($this->facets) as $n => $key) {
            $query = $this->pdo->query("
                SELECT meta_$n AS value, count(*) AS count 
                FROM " . self::TEMPTABNAME . "
                GROUP BY 1 
                ORDER BY 2 DESC
            ");
            while ($row   = $query->fetchObject()) {
                $stats[$key][$row->value] = $row->count;
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
}
