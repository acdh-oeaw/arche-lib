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

use Generator;
use PDO;
use PDOException;
use PDOStatement;
use EasyRdf\Graph;
use EasyRdf\Literal;
use zozlak\RdfConstants as RDF;
use zozlak\queryPart\QueryPart;
use acdhOeaw\arche\lib\RepoResourceInterface AS RRI;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Provides a read only access to the repository on the relational database level.
 *
 * @author zozlak
 */
class RepoDb implements RepoInterface {

    use RepoTrait;

    /**
     * A class used to instantiate objects representing repository resources.
     * 
     * To be used by external libraries extending the RepoResource class funcionality provided by this library.
     * 
     * @var string
     */
    static public $resourceClass = '\acdhOeaw\arche\lib\RepoResourceDb';

    /**
     * Creates a repository object instance from a given configuration file.
     * 
     * Automatically parses required config properties and passes them to the RepoDb object constructor.
     * 
     * At the moment it doesn't support authorization provider instantiating.
     * 
     * @param string $configFile a path to the YAML config file
     * @param string $dbSettings database connection variant to read from the config
     * @return RepoDb
     */
    static public function factory(string $configFile,
                                   string $dbSettings = 'guest'): RepoDb {
        $config = Config::fromYaml($configFile);

        $baseUrl    = $config->rest->urlBase . $config->rest->pathBase;
        $schema     = new Schema($config->schema);
        $headers    = new Schema($config->rest->headers);
        $pdo        = new PDO($config->dbConn->$dbSettings);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $nonRelProp = $config->metadataManagment->nonRelationProperties ?? [];
        return new RepoDb($baseUrl, $schema, $headers, $pdo, (array) $nonRelProp);
    }

    private PDO $pdo;
    private ?AuthInterface $auth;

    /**
     *
     * @var array<string>
     */
    private array $nonRelationProperties;

    /**
     * 
     * @param string $baseUrl
     * @param Schema $schema
     * @param Schema $headers
     * @param PDO $pdo
     * @param array<string> $nonRelationProperties
     * @param AuthInterface $auth
     */
    public function __construct(string $baseUrl, Schema $schema,
                                Schema $headers, PDO $pdo,
                                array $nonRelationProperties = [],
                                ?AuthInterface $auth = null) {
        $this->baseUrl               = $baseUrl;
        $this->schema                = $schema;
        $this->headers               = $headers;
        $this->pdo                   = $pdo;
        $this->nonRelationProperties = $nonRelationProperties;
        $this->auth                  = $auth;
    }

    /**
     * Tries to find a single repository resource matching provided identifiers.
     * 
     * A resource matches the search if at lest one id matches the provided list.
     * Resource is not required to have all provided ids.
     * 
     * If more then one resources matches the search or there is no resource
     * matching the search, an error is thrown.
     * 
     * @param array<string> $ids an array of identifiers (being strings)
     * @param string $class an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @return RepoResourceDb
     * @throws NotFound
     * @throws AmbiguousMatch
     */
    public function getResourceByIds(array $ids, string $class = null): RepoResourceDb {
        $placeholders = substr(str_repeat('?, ', count($ids)), 0, -2);
        $query        = "SELECT DISTINCT id FROM identifiers WHERE ids IN ($placeholders)";
        $query        = $this->pdo->prepare($query);
        $query->execute($ids);
        $id           = $query->fetchColumn();
        if ($id === false) {
            throw new NotFound();
        }
        if (($id2 = $query->fetchColumn()) !== false) {
            throw new AmbiguousMatch("Both resource $id and $id2 match the search");
        }
        $url   = $this->getBaseUrl() . $id;
        $class = $class ?? self::$resourceClass;
        $obj   = new $class($url, $this);
        /** @var RepoResourceDb $obj */
        return $obj;
    }

    /**
     * Returns repository resources matching all provided search terms.
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return Generator<RepoResourceDb>
     */
    public function getResourcesBySearchTerms(array $searchTerms,
                                              SearchConfig $config): Generator {
        $graph = $this->getGraphBySearchTerms($searchTerms, $config);
        yield from $this->parseSearchGraph($graph, $config);
    }

    /**
     * Performs a search
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return Generator<RepoResourceDb>
     */
    public function getResourcesBySqlQuery(string $query, array $parameters,
                                           SearchConfig $config): Generator {
        $graph = $this->getGraphBySqlQuery($query, $parameters, $config);
        yield from $this->parseSearchGraph($graph, $config);
    }

    /**
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return Graph
     */
    public function getGraphBySearchTerms(array $searchTerms,
                                          SearchConfig $config): Graph {
        $query         = $this->getPdoStatementBySearchTerms($searchTerms, $config);
        $graph         = $this->parsePdoStatement($query);
        $config->count = (int) ((string) $graph->resource($this->getBaseUrl())->getLiteral($this->schema->searchCount));
        return $graph;
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return Graph
     */
    public function getGraphBySqlQuery(string $query, array $parameters,
                                       SearchConfig $config): Graph {
        $query         = $this->getPdoStatementBySqlQuery($query, $parameters, $config);
        $graph         = $this->parsePdoStatement($query);
        $config->count = (int) ((string) $graph->resource($this->getBaseUrl())->getLiteral($this->schema->searchCount));
        return $graph;
    }

    /**
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return PDOStatement
     */
    public function getPdoStatementBySearchTerms(array $searchTerms,
                                                 SearchConfig $config): PDOStatement {
        $query = '';
        $param = [];
        $many  = count($searchTerms) > 1;
        foreach ($searchTerms as $n => $term) {
            $qpTmp = $term->getSqlQuery($this->getBaseUrl(), $this->schema->id, $this->nonRelationProperties);
            if (empty($query)) {
                $query = ($many ? "(" : "") . $qpTmp->query . ($many ? ") t$n" : "");
            } else {
                $query .= " JOIN ($qpTmp->query) t$n USING (id) ";
            }
            $param = array_merge($param, (array) $qpTmp->param);
        }
        return $this->getPdoStatementBySqlQuery($query, $param, $config);
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return PDOStatement
     * @throws RepoLibException
     */
    public function getPdoStatementBySqlQuery(string $query, array $parameters,
                                              SearchConfig $config): PDOStatement {
        $authQP   = $this->getMetadataAuthQuery();
        $pagingQP = $this->getPagingQuery($config);
        list($orderByQP1, $orderByQP2) = $this->getOrderByQuery($config);

        $ftsWithQp       = new QueryPart();
        $searchMetaQuery = '';
        $searchMetaParam = [];
        if ($config->skipArtificialProperties === false) {
            list($ftsWithQp, $ftsQP) = $this->getFtsQuery($config);
            $searchMetaQuery = "
              UNION
                SELECT id, ?::text AS property, ?::text AS type, ''::text AS lang, ?::text AS value FROM ids
              UNION
                SELECT null::bigint, ?::text AS property, ?::text AS type, ''::text AS lang, count(*)::text AS value FROM allids
                $ftsQP->query
                $orderByQP2->query
            ";
            $searchMetaParam = array_merge(
                [
                    $this->schema->searchMatch, RDF::XSD_BOOLEAN, 'true',
                    $this->schema->searchCount, RDF::XSD_INTEGER,
                ],
                $ftsQP->param, $orderByQP2->param,
            );
            foreach ($orderByQP1->columns as $n => $i) {
                $searchMetaQuery   .= "UNION
                    SELECT id, ?::text AS property, ?::text AS type, ''::text AS lang, $i AS value FROM ids
                ";
                $searchMetaParam[] = $this->schema->searchOrderValue . ($n + 1);
                $searchMetaParam[] = RDF::XSD_STRING;
            }
        }

        $mode = $config->metadataMode ?? '';
        switch ($mode) {
            case RRI::META_NONE:
                $metaQuery   = "SELECT * FROM metadata WHERE false";
                $metaParam   = [];
            case RRI::META_IDS:
                $metaQuery   = "SELECT id, property, type, lang, value FROM metadata JOIN ids USING (id) WHERE property = ?";
                $metaParam   = [$this->schema->label];
                break;
            default:
                $getRelParam = $this->parseMetadataReadMode($mode);
                $metaQuery   = "
                    , relatives AS (SELECT DISTINCT (get_relatives(id::bigint, ?::text, ?::int, ?::int, ?::bool, ?::bool)).id FROM ids),
                    meta AS (
                        SELECT id, ?::text AS property, 'ID'::text AS type, null::text AS lang, ids AS value, false AS revrel
                        FROM relatives JOIN identifiers USING (id)
                        UNION
                        SELECT r.id, property, 'REL'::text AS type, null::text AS lang, target_id::text AS value, i.id IS NOT NULL as revrel
                        FROM relatives JOIN relations r USING (id) LEFT JOIN ids i ON r.target_id = i.id 
                        UNION
                        SELECT id, property, type, lang, value, false AS revrel
                        FROM relatives JOIN metadata USING (id)
                    )
                ";
                $metaParam   = array_merge(
                    [$config->metadataParentProperty],
                    $getRelParam,
                    [$this->schema->id]
                );
                $metaWhere   = '';
                // filter output properties
                if (count($config->resourceProperties) > 0) {
                    $metaWhere .= " OR ids.id IS NOT NULL AND property IN (" . substr(str_repeat(', ?', count($config->resourceProperties)), 2) . ")";
                    $metaParam = array_merge($metaParam, $config->resourceProperties);
                } elseif (count($config->relativesProperties) > 0) {
                    $metaWhere .= " OR ids.id IS NOT NULL";
                }
                if (count($config->relativesProperties) > 0) {
                    $metaWhere .= " OR ids.id IS NULL AND (property IN (" . substr(str_repeat(', ?', count($config->relativesProperties)), 2) . ") OR revrel)";
                    $metaParam = array_merge($metaParam, $config->relativesProperties);
                } elseif (count($config->resourceProperties) > 0) {
                    $metaWhere .= " OR ids.id IS NULL";
                }
                if (empty($metaWhere)) {
                    $metaQuery .= "SELECT id, property, type, lang, value FROM meta";
                } else {
                    $metaQuery .= "SELECT meta.id, meta.property, meta.type, meta.lang, meta.value FROM meta LEFT JOIN ids USING (id) WHERE " . substr($metaWhere, 4);
                }
        }

        $orderByCols = '';
        if (count($orderByQP1->columns) > 0) {
            $orderByCols = ', ' . implode(', ', $orderByQP1->columns);
        }
        $query = "
            WITH
                allids AS (
                    SELECT id FROM ($query) t $authQP->query
                ),
                ids AS (
                    SELECT id $orderByCols FROM allids
                    $orderByQP1->query
                    $pagingQP->query
                )
                $ftsWithQp->query
            $metaQuery
            $searchMetaQuery
        ";
        $param = array_merge($parameters, $authQP->param, $orderByQP1->param, $pagingQP->param, $ftsWithQp->param, $metaParam, $searchMetaParam);
        $this->logQuery($query, $param);

        $query = $this->pdo->prepare($query);
        try {
            $query->execute($param);
        } catch (PDOException $e) {
            if (isset($this->queryLog)) {
                $this->queryLog->error($e);
            }
            throw new RepoLibException('Bad query', 400, $e);
        }
        return $query;
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $param
     * @return PDOStatement
     */
    public function runQuery(string $query, array $param): PDOStatement {
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        return $query;
    }

    /**
     * 
     * @return QueryPart
     */
    public function getMetadataAuthQuery(): QueryPart {
        if ($this->auth !== null) {
            return $this->auth->getMetadataAuthQuery();
        }
        return new QueryPart();
    }

    /**
     * Prepares an SQL query adding a full text search query results as 
     * metadata graph edges.
     * 
     * @return array<QueryPart>
     */
    private function getFtsQuery(SearchConfig $cfg): array {
        $query     = '';
        $param     = [];
        $withQuery = '';
        $withParam = [];
        if (!empty($cfg->ftsQuery)) {
            $withQuery = "
                , fts AS (
                    SELECT 
                        coalesce(fts.id, fts.iid, m.id) AS id,
                        CASE 
                            WHEN fts.mid IS NOT NULL THEN m.property 
                            WHEN fts.iid IS NOT NULL THEN ?::text 
                            ELSE ?::text
                        END AS property,
                        coalesce(m.lang, '') AS lang,
                        ts_headline('simple', raw, websearch_to_tsquery('simple', ?), ?) AS value,
                        row_number() OVER (PARTITION BY coalesce(fts.id, fts.iid, m.id)) AS no
                    FROM 
                        full_text_search fts 
                        LEFT JOIN metadata m USING (mid) 
                        JOIN ids i ON i.id = coalesce(fts.id, fts.iid, m.id)
                    WHERE
                        websearch_to_tsquery('simple', ?) @@ segments
            ";
            $withParam = [
                $this->schema->id,
                SearchTerm::PROPERTY_BINARY,
                $cfg->ftsQuery,
                $cfg->getTsHeadlineOptions(),
                $cfg->ftsQuery,
            ];

            $cfg->ftsProperty ??= [];
            if (!is_array($cfg->ftsProperty)) {
                $cfg->ftsProperty = [$cfg->ftsProperty];
            }
            if (count($cfg->ftsProperty) > 0) {
                $withQuery .= "AND (property IN (" . substr(str_repeat(', ?', count($cfg->ftsProperty)), 2) . ")";
                $withParam = array_merge($withParam, $cfg->ftsProperty);
                if (in_array($this->schema->id, $cfg->ftsProperty)) {
                    $withQuery .= " OR fts.iid IS NOT NULL";
                }
                if (in_array(SearchTerm::PROPERTY_BINARY, $cfg->ftsProperty)) {
                    $withQuery .= " OR fts.id IS NOT NULL";
                }
                $withQuery .= ")\n";
            }

            $withQuery .= ")";

            $query = "
              UNION
                SELECT id, ?::text || no::text AS property, ?::text AS type, lang, value
                FROM fts
              UNION
                SELECT id, ?::text || no::text AS property, ?::text AS type, '' AS lang, property AS value
                FROM fts
            ";
            $param = [
                $this->schema->searchFts, RDF::XSD_STRING,
                $this->schema->searchFtsProperty, RDF::XSD_STRING,
            ];
        }
        return [new QueryPart($withQuery, $withParam), new QueryPart($query, $param)];
    }

    /**
     * 
     * @param SearchConfig $config
     * @return array<QueryPart>
     */
    private function getOrderByQuery(SearchConfig $config): array {
        $qp = new QueryPart();
        if (!is_array($config->orderBy) || count($config->orderBy) === 0) {
            return [$qp, $qp];
        }
        $lang      = !empty($config->orderByLang) ? ", lang = ? DESC NULLS LAST" : '';
        $collation = '';
        if (!empty($config->orderByCollation)) {
            $query = $this->pdo->prepare("SELECT count(*) FROM pg_collation WHERE collname = ?");
            $query->execute([$config->orderByCollation]);
            if ($query->fetchColumn() !== 1) {
                throw new RepoLibException("Unsupported collation '$config->orderByCollation'", 400);
            }
            $collation = 'COLLATE "' . $config->orderByCollation . '"';
        }
        $orderBy = '';
        ksort($config->orderBy);
        foreach (array_values($config->orderBy) as $n => $property) {
            $desc = '';
            if (substr($property, 0, 1) === '^') {
                $desc     = 'DESC';
                $property = substr($property, 1);
            }
            $qp->query     .= "
                LEFT JOIN (
                    SELECT DISTINCT ON (id) id, value AS _ob$n, value_t AS _obt$n, value_n AS _obn$n
                    FROM metadata m
                    WHERE
                        property = ?
                        AND EXISTS (SELECT 1 FROM allids WHERE id = m.id)
                    ORDER BY id $lang, value_t, value_n, value
                ) t$n USING (id)
            ";
            $qp->param[]   = $property;
            $qp->columns[] = "_ob$n";
            if (!empty($config->orderByLang)) {
                $qp->param[] = $config->orderByLang;
            }
            $orderBy .= ($n > 0 ? ', ' : '') . "_obt$n $desc NULLS LAST, _obn$n $desc NULLS LAST, _ob$n $collation $desc NULLS LAST";
        }
        $qp->query .= "ORDER BY $orderBy\n";

        $qp2        = new QueryPart();
        $qp2->query = "
            UNION
            SELECT ids.id, ? AS property, ? AS type, '' AS lang, (row_number() OVER ())::text AS value 
            FROM ids
        ";
        $qp2->param = [$this->schema->searchOrder, RDF::XSD_POSITIVE_INTEGER];
        return [$qp, $qp2];
    }

    /**
     * 
     * @param SearchConfig $config
     * @return QueryPart
     */
    private function getPagingQuery(SearchConfig $config): QueryPart {
        $query = '';
        $param = [];
        if ($config->limit !== null) {
            $query   .= ' LIMIT ?';
            $param[] = $config->limit;
        }
        if ($config->offset !== null) {
            $query   .= ' OFFSET ?';
            $param[] = $config->offset;
        }
        return new QueryPart($query, $param);
    }

    /**
     * Parses SQL query results containing resources metadata into an RDF graph.
     * 
     * @param PDOStatement $query
     * @return Graph
     */
    public function parsePdoStatement(PDOStatement $query): Graph {
        $idProp  = $this->schema->id;
        $baseUrl = $this->getBaseUrl();
        $graph   = new Graph();
        while ($triple  = $query->fetchObject()) {
            $triple->id = $baseUrl . $triple->id;
            $resource   = $graph->resource($triple->id);
            switch ($triple->type) {
                case 'ID':
                    $resource->addResource($idProp, $triple->value);
                    break;
                case 'REL':
                    $resource->addResource($triple->property, $baseUrl . $triple->value);
                    break;
                case 'URI':
                    $resource->addResource($triple->property, $triple->value);
                    break;
                case 'GEOM':
                    $triple->type = RDF::XSD_STRING;
                default:
                    $type         = empty($triple->lang) & $triple->type !== RDF::XSD_STRING ? $triple->type : null;
                    $literal      = new Literal($triple->value, !empty($triple->lang) ? $triple->lang : null, $type);
                    $resource->add($triple->property, $literal);
            }
        }
        return $graph;
    }

    /**
     * 
     * @param Graph $graph
     * @param SearchConfig $config
     * @return Generator<RepoResourceDb>
     */
    private function parseSearchGraph(Graph $graph, SearchConfig $config): Generator {
        $class = $config->class ?? RepoResourceDb::class;

        $resources = $graph->resourcesMatching($this->schema->searchMatch);
        $this->sortMatchingResources($resources, $this->schema->searchOrder);
        foreach ($resources as $i) {
            $i->delete($this->schema->searchMatch);
            $i->delete($this->schema->searchOrder);
            $obj = new $class($i->getUri(), $this);
            /** @var RepoResourceDb $obj */
            $obj->setGraph($i);
            yield $obj;
        }
    }

    /**
     * @param string $mode
     * @return array<int>
     */
    private function parseMetadataReadMode(string $mode): array {
        $param = match ($mode) {
            RRI::META_RESOURCE => [0, 0, 0, 0],
            RRI::META_NEIGHBORS => [0, 0, 1, 1],
            RRI::META_RELATIVES => [999999, -999999, 1, 0],
            RRI::META_RELATIVES_ONLY => [999999, -999999, 0, 0],
            RRI::META_RELATIVES_REVERSE => [999999, -999999, 1, 1],
            RRI::META_PARENTS => [0, -999999, 1, 0],
            RRI::META_PARENTS_ONLY => [0, -999999, 0, 0],
            RRI::META_PARENTS_REVERSE => [0, -999999, 1, 1],
            default => null,
        };
        if ($param === null) {
            $checkFn = fn($x) => is_numeric($x) ? (int) $x : throw new RepoLibException('Bad metadata mode ' . $mode, 400);
            $param   = array_map($checkFn, explode('_', $mode));
            $param   = array_merge($param, array_fill(0, 4 - count($param), 0));
            if ($param[2] < 0 || $param[2] > 1 || $param[3] < 0 || $param[3] > 1 || count($param) !== 4) {
                throw new RepoLibException('Bad metadata mode ' . $mode, 400);
            }
            $param[1] = -$param[1];
        }
        return $param;
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $param
     * @return void
     */
    private function logQuery(string $query, array $param): void {
        if (isset($this->queryLog)) {
            $this->queryLog->debug("\tSearch query:\n" . (new QueryPart($query, $param)));
        }
    }
}
