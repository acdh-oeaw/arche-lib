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

use PDOStatement;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Provides a read-only access to the repository resource's metadata.
 * 
 * @author zozlak
 */
class RepoResourceDb implements RepoResourceInterface {

    use RepoResourceTrait;

    private int $id;
    private RepoDb $repo;

    /**
     * Creates an object representing a repository resource.
     * 
     * @param string $urlOrId either a resource URL or just the numeric id
     * @param RepoInterface $repo repository connection object
     */
    public function __construct(string $urlOrId, RepoInterface $repo) {
        if (!$repo instanceof RepoDb) {
            throw new RepoLibException('The RepoResourceDb object can be created only with a RepoDb repository connection handle');
        }
        if (!is_numeric($urlOrId)) {
            $urlOrId = preg_replace('/^.*[^0-9]/', '', $urlOrId);
        }
        $this->id      = (int) $urlOrId;
        $this->repo    = $repo;
        $this->repoInt = $repo;
        $this->url     = $this->repo->getBaseUrl() . $this->id;
    }

    /**
     * Loads current metadata from the repository.
     * 
     * @param bool $force enforce fetch from the repository 
     *   (when you want to make sure metadata are in line with ones in the repository 
     *   or e.g. reset them back to their current state in the repository)
     * @param string $mode scope of the metadata returned by the repository - see the 
     *   `RepoDb()::getPdoStatementBySqlQuery()` method
     * @param string $parentProperty RDF property name used to find related resources 
     *   - see the getMetadataQuery() method
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @return void
     * @throws RepoLibException
     * @see getMetadataQuery
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 string $parentProperty = null,
                                 array $resourceProperties = [],
                                 array $relativesProperties = []): void {
        if (!$force && $this->metadata !== null) {
            return;
        }
        $stmt           = $this->getMetadataStatement($mode, $parentProperty, $resourceProperties, $relativesProperties);
        $graph          = $this->repo->parsePdoStatement($stmt);
        $this->metadata = $graph->resource($this->getUri());
    }

    /**
     * Returns a QueryPart object with an SQL query loading resource's metadata
     * in a given mode.
     * 
     * @param string $mode scope of the metadata returned by the repository - see the 
     *   `RepoDb()::getPdoStatementBySqlQuery()` method
     * @param string $parentProperty RDF property name used to find related resources
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @return PDOStatement
     */
    public function getMetadataStatement(string $mode = self::META_RESOURCE,
                                         string $parentProperty = null,
                                         array $resourceProperties = [],
                                         array $relativesProperties = []): PDOStatement {
        $config                           = new SearchConfig();
        $config->metadataMode             = $mode;
        $config->metadataParentProperty   = $parentProperty;
        $config->resourceProperties       = $resourceProperties;
        $config->relativesProperties      = $relativesProperties;
        $config->skipArtificialProperties = true;

        $term = new SearchTerm(null, $this->id, '=', SearchTerm::TYPE_ID);
        return $this->repo->getPdoStatementBySearchTerms([$term], $config);
    }
}
