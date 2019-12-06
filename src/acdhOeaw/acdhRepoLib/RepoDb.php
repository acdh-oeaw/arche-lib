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

use PDO;
use PDOException;
use PDOStatement;
use EasyRdf\Graph;
use EasyRdf\Literal;
use zozlak\RdfConstants as RDF;
use acdhOeaw\acdhRepoLib\RepoResourceInterface AS RRI;
use acdhOeaw\acdhRepoLib\exception\AmbiguousMatch;
use acdhOeaw\acdhRepoLib\exception\NotFound;
use acdhOeaw\acdhRepoLib\exception\RepoLibException;

/**
 * Provides a read only access to the repository on the relational database level.
 *
 * @author zozlak
 */
class RepoDb implements RepoInterface {

    use RepoTrait;

    static private $highlightParam = [
        'StartSel', 'StopSel', 'MaxWords', 'MinWords',
        'ShortWord', 'HighlightAll', 'MaxFragments', 'FragmentDelimiter'
    ];

    /**
     *
     * @var \PDO
     */
    private $pdo;

    /**
     *
     * @var string[]
     */
    private $nonRelationProperties;

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\AuthInterface
     */
    private $auth;

    /**
     * 
     * @param string $baseUrl
     * @param \acdhOeaw\acdhRepoLib\Schema $schema
     * @param \acdhOeaw\acdhRepoLib\Schema $headers
     * @param \PDO $pdo
     * @param string[] $nonRelationProperties
     * @param \acdhOeaw\acdhRepoLib\AuthInterface $auth
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
     * @param array $ids an array of identifiers (being strings)
     * @param string $class an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     * @throws NotFound
     * @throws AmbiguousMatch
     */
    public function getResourceByIds(array $ids, string $class = null): RepoResourceInterface {
        $placeholders = substr(str_repeat('?, ', count($ids)), 0, -2);
        $query        = "SELECT DISTINCT id FROM identifiers WHERE ids IN ($placeholders)";
        $query        = $this->pdo->prepare($query);
        $query->execute($ids);
        $id           = $query->fetchColumn();
        if ($id === false) {
            throw new NotFound();
        }
        if ($query->fetchColumn() !== false) {
            throw new AmbiguousMatch();
        }
        $url = $this->getBaseUrl() . $id;
        return new $class($url, $this);
    }

    /**
     * Returns repository resources matching all provided search terms.
     * 
     * @param array $searchTerms
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \acdhOeaw\acdhRepoLib\RepoResourceInterface[]
     */
    public function getResourcesBySearchTerms(array $searchTerms,
                                              SearchConfig $config): array {
        $graph = $this->getGraphBySearchTerms($searchTerms, $config);
        return $this->parseSearchGraph($graph, $config->class);
    }

    /**
     * Performs a search
     * 
     * @param string $query
     * @param array $parameters
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \acdhOeaw\acdhRepoLib\RepoResourceInterface[]
     */
    public function getResourcesBySqlQuery(string $query, array $parameters,
                                           SearchConfig $config): array {
        $graph = $this->getGraphBySqlQuery($query, $parameters, $config);
        return $this->parseSearchGraph($graph, $config->class);
    }

    /**
     * 
     * @param array $searchTerms
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \EasyRdf\Graph
     */
    public function getGraphBySearchTerms(array $searchTerms,
                                          SearchConfig $config): Graph {
        $query = $this->getPdoStatementBySearchTerms($searchTerms, $config);
        return $this->parsePdoStatement($query);
    }

    /**
     * 
     * @param string $query
     * @param array $parameters
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \EasyRdf\Graph
     */
    public function getGraphBySqlQuery(string $query, array $parameters,
                                       SearchConfig $config): Graph {
        $query = $this->getPdoStatementBySqlQuery($query, $parameters, $config);
        return $this->parsePdoStatement($query);
    }

    /**
     * 
     * @param array $searchTerms
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \PDOStatement
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
            $param = array_merge($param, $qpTmp->param);
        }
        return $this->getPdoStatementBySqlQuery($query, $param, $config);
    }

    /**
     * 
     * @param string $query
     * @param array $parameters
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \PDOStatement
     * @throws RepoLibException
     */
    public function getPdoStatementBySqlQuery(string $query, array $parameters,
                                              SearchConfig $config): PDOStatement {
        $authQP   = $this->getMetadataAuthQuery();
        $pagingQP = $this->getPagingQuery($config);
        $ftsQP    = $this->getFtsQuery($config);

        switch (strtolower($config->metadataMode)) {
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
                $metaQuery = "SELECT (get_relatives_metadata(id, ?)).* FROM ids";
                $metaParam = [$config->metadataParentProperty];
                break;
            default:
                throw new RepoLibException('Wrong metadata read mode value ' . $config->metadataMode, 400);
        }

        $query       = "
            WITH ids AS (
                SELECT id FROM (" . $query . ") t1 " . $authQP->query . " $pagingQP->query
            )
            $metaQuery
            UNION
            SELECT id, ?::text AS property, ?::text AS type, ''::text AS lang, ?::text AS value FROM ids
            $ftsQP->query
        ";
        $schemaParam = [$this->getSchema()->searchMatch, RDF::XSD_BOOLEAN, 'true'];
        $param       = array_merge($parameters, $authQP->param, $pagingQP->param, $metaParam, $schemaParam, $ftsQP->param);
        $this->logQuery($query, $param);

        $query = $this->pdo->prepare($query);
        try {
            $query->execute($param);
        } catch (PDOException $e) {
            if ($this->queryLog) {
                $this->queryLog->error($e);
            }
            throw new RepoLibException('Bad query', 400, $e);
        }
        return $query;
    }

    /**
     * 
     * @param string $query
     * @param array $param
     * @return \PDOStatement
     */
    public function runQuery(string $query, array $param): PDOStatement {
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        return $query;
    }

    /**
     * 
     * @return \acdhOeaw\acdhRepoLib\QueryPart
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
     * @return \acdhOeaw\acdhRepoLib\QueryPart
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

            $where      = '';
            $whereParam = [];
            if (!empty($cfg->ftsProperty)) {
                $where        = "WHERE property = ?";
                $whereParam[] = $cfg->ftsProperty;
            }

            $query = "
              UNION
                SELECT id, ? AS property, ? AS type, '' AS lang, ts_headline('simple', raw, websearch_to_tsquery('simple', ?), ?) AS value 
                FROM full_text_search JOIN ids USING (id)
                $where
            ";
            $prop  = $this->getSchema()->searchFts;
            $type  = RDF::XSD_STRING;
            $param = array_merge([$prop, $type, $cfg->ftsQuery, $options], $whereParam);
        }
        return new QueryPart($query, $param);
    }

    /**
     * 
     * @param \acdhOeaw\acdhRepoLib\SearchConfig $config
     * @return \acdhOeaw\acdhRepoLib\QueryPart
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
     * @return \EasyRdf\Graph
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
                default:
                    $type    = empty($triple->lang) & $triple->type !== RDF::XSD_STRING ? $triple->type : null;
                    $literal = new Literal($triple->value, !empty($triple->lang) ? $triple->lang : null, $type);
                    $resource->add($triple->property, $literal);
            }
        }
        return $graph;
    }

    /**
     * 
     * @param \EasyRdf\Graph $graph
     * @param string $class
     * @return \acdhOeaw\acdhRepoLib\RepoResourceInterface[]
     */
    private function parseSearchGraph(Graph $graph, string $class): array {
        $resources = $graph->resourcesMatching($this->schema->searchMatch);
        $objects   = [];
        foreach ($resources as $i) {
            $obj       = new $class($i->getUri(), $this);
            $obj->setGraph($i);
            $objects[] = $obj;
        }
        return $objects;
    }

    /**
     * 
     * @param string $query
     * @param array $param
     * @return void
     */
    private function logQuery(string $query, array $param): void {
        if ($this->queryLog !== null) {
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
