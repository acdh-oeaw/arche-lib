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
     * 
     * @var array<string>
     */
    static private array $highlightParam = [
        'StartSel', 'StopSel', 'MaxWords', 'MinWords',
        'ShortWord', 'HighlightAll', 'MaxFragments', 'FragmentDelimiter'
    ];

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
        return new $class($url, $this);
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
        $config->count = (int) ((string) $graph->resource($this->getBaseUrl())->getLiteral($this->getSchema()->searchCount));
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
        $config->count = (int) ((string) $graph->resource($this->getBaseUrl())->getLiteral($this->getSchema()->searchCount));
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
        $authQP    = $this->getMetadataAuthQuery();
        $pagingQP  = $this->getPagingQuery($config);
        $ftsQP     = $this->getFtsQuery($config);
        $orderByQP = $this->getOrderByQuery($config);

        $mode = $config->metadataMode ?? '';
        switch ($mode) {
            case RRI::META_RESOURCE:
                $metaQuery = "
                    SELECT id, property, type, lang, value
                    FROM metadata JOIN ids USING (id)
                  UNION
                    SELECT id, null, 'ID' AS type, null, ids AS VALUE 
                    FROM identifiers JOIN ids USING (id)
                  UNION
                    SELECT id, property, 'REL' AS type, null, target_id::text AS value
                    FROM relations JOIN ids USING (id)
                ";
                $metaParam = [];
                break;
            case RRI::META_NEIGHBORS:
                $metaQuery = "SELECT (get_neighbors_metadata(id, ?)).* FROM ids";
                $metaParam = [$config->metadataParentProperty];
                break;
            case RRI::META_RELATIVES:
            case RRI::META_RELATIVES_ONLY:
            case RRI::META_RELATIVES_REVERSE:
            case RRI::META_PARENTS:
            case RRI::META_PARENTS_ONLY:
            case RRI::META_PARENTS_REVERSE:
                $max       = $mode === RRI::META_PARENTS || $mode === RRI::META_PARENTS_ONLY || $mode === RRI::META_PARENTS_REVERSE ? 0 : 999999;
                $neighbors = $mode === RRI::META_PARENTS_ONLY || $mode === RRI::META_RELATIVES_ONLY ? false : true;
                $reverse   = $mode === RRI::META_PARENTS_REVERSE || $mode === RRI::META_RELATIVES_REVERSE ? true : false;
                $metaQuery = "SELECT (get_relatives_metadata(id, ?, ?, -999999, ?, ?)).* FROM ids";
                $metaParam = [
                    $config->metadataParentProperty, $max, (int) $neighbors, (int) $reverse
                ];
                break;
            case RRI::META_NONE:
            case RRI::META_IDS:
                $metaQuery = "SELECT id, property, type, lang, value FROM metadata JOIN ids USING (id) WHERE property = ?";
                $metaParam = [$this->schema->label];
                break;
            default:
                throw new RepoLibException('Wrong metadata read mode value ' . $config->metadataMode, 400);
        }

        $query       = "
            WITH
                allids AS (
                    SELECT id FROM ($query) t $authQP->query
                ),
                ids AS (
                    SELECT id FROM allids
                        $orderByQP->query
                    $pagingQP->query
                )
            $metaQuery
            UNION
            SELECT id, ?::text AS property, ?::text AS type, ''::text AS lang, ?::text AS value FROM ids
            UNION
            SELECT null::bigint, ?::text AS property, ?::text AS type, ''::text AS lang, count(*)::text AS value FROM allids
            $ftsQP->query
        ";
        $schemaParam = [
            $this->getSchema()->searchMatch, RDF::XSD_BOOLEAN, 'true',
            $this->getSchema()->searchCount, RDF::XSD_INTEGER,
        ];
        $param       = array_merge($parameters, $authQP->param, $orderByQP->param, $pagingQP->param, $metaParam, $schemaParam, $ftsQP->param);
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
     * @return QueryPart
     */
    private function getFtsQuery(SearchConfig $cfg): QueryPart {
        $query = '';
        $param = [];
        if (!empty($cfg->ftsQuery)) {
            $options = '';
            foreach (self::$highlightParam as $i) {
                $ii = 'fts' . $i;
                if ($cfg->$ii !== null) {
                    $options .= " ,$i=" . $cfg->$ii;
                }
            }
            $options = substr($options, 2);

            $join       = 'JOIN ids USING (id)';
            $idSrc      = 'fts';
            $where      = '';
            $whereParam = [];
            if (!empty($cfg->ftsProperty) && $cfg->ftsProperty !== SearchTerm::PROPERTY_BINARY) {
                $where        = "WHERE property = ?";
                $whereParam[] = $cfg->ftsProperty;
                $join         = "JOIN metadata m USING (mid) JOIN ids ON m.id = ids.id";
                $idSrc        = 'm';
            }

            $query = "
              UNION
                SELECT $idSrc.id, ? AS property, ? AS type, '' AS lang, ts_headline('simple', raw, websearch_to_tsquery('simple', ?), ?) AS value 
                FROM full_text_search fts $join
                $where
            ";
            $prop  = $this->getSchema()->searchFts;
            $type  = RDF::XSD_STRING;
            $param = array_merge([$prop, $type, $cfg->ftsQuery, $options], $whereParam);
        }
        return new QueryPart($query, $param);
    }

    private function getOrderByQuery(SearchConfig $config): QueryPart {
        $qp = new QueryPart();
        if (!is_array($config->orderBy) || count($config->orderBy) === 0) {
            return $qp;
        }
        $lang    = !empty($config->orderByLang) ? "AND (type <> ? OR lang = ?)" : '';
        $orderBy = '';
        foreach ($config->orderBy as $n => $property) {
            $desc = '';
            if (substr($property, 0, 1) === '^') {
                $desc     = 'DESC';
                $property = substr($property, 1);
            }
            $qp->query   .= "LEFT JOIN (SELECT id, min(value) AS _ob$n FROM metadata WHERE property = ? $lang GROUP BY 1) t$n USING (id)\n";
            $qp->param[] = $property;
            if (!empty($config->orderByLang)) {
                $qp->param[] = RDF::XSD_STRING;
                $qp->param[] = $config->orderByLang;
            }
            $orderBy .= ($n > 0 ? ', ' : '') . "_ob$n $desc NULLS LAST";
        }
        $qp->query .= "ORDER BY $orderBy\n";
        return $qp;
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
        $idProp  = $this->getSchema()->id;
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
        if (count($config->orderBy) > 0) {
            $this->sortMatchingResources($resources, $config);
        }
        foreach ($resources as $i) {
            $i->delete($this->schema->searchMatch);
            $obj = new $class($i->getUri(), $this);
            $obj->setGraph($i);
            yield $obj;
        }
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $param
     * @return void
     */
    private function logQuery(string $query, array $param): void {
        if (isset($this->queryLog)) {
            $msg = "\tSearch query:\n";
            while (($pos = strpos($query, '?')) !== false) {
                $msg   .= substr($query, 0, $pos) . $this->pdo->quote(array_shift($param));
                $query = substr($query, $pos + 1);
            }
            $msg .= $query;
            $this->queryLog->debug("\tSearch query:\n" . $msg);
        }
    }
}
