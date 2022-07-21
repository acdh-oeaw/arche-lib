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

use zozlak\queryPart\QueryPart;
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
     * @param string $mode scope of the metadata returned by the repository - see the getMetadataQuery() method
     * @param string $parentProperty RDF property name used to find related resources - see the getMetadataQuery() method
     * @return void
     * @throws RepoLibException
     * @see getMetadataQuery
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 ?string $parentProperty = null): void {
        if (!$force && $this->metadata !== null) {
            return;
        }
        $queryQP        = $this->getMetadataQuery($mode, $parentProperty);
        $query          = $this->repo->runQuery($queryQP->query, $queryQP->param);
        $graph          = $this->repo->parsePdoStatement($query);
        $this->metadata = $graph->resource($this->getUri());
    }

    /**
     * Returns a QueryPart object with an SQL query loading resource's metadata
     * in a given mode.
     * 
     * @param string $mode scope of the metadata returned by the repository: 
     *   `RepoResourceInterface::META_RESOURCE` - only given resource metadata,
     *   `RepoResourceInterface::META_NEIGHBORS` - metadata of a given resource, all the resources pointed by its metadata and all its children
     *      (resource pointing to a given resource with the `$parentProperty` property),
     *   `RepoResourceInterface::META_RELATIVES` - metadata of a given resource and all resources recursively pointing to a given metadata property
     *      (see the `$parentProperty` parameter) in both directions (both "parents" and "children")
     *   `RepoResourceInterface::META_PARENTS` - like META_RELATIVES but only parents are returned
     * @param string|null $parentProperty RDF property name used to find related resources in the 
     *   `RepoResourceInterface::META_RELATIVES` and `RepoResourceInterface::META_PARENTS` modes
     * @return QueryPart
     * @throws RepoLibException
     */
    public function getMetadataQuery(string $mode = self::META_RESOURCE,
                                     ?string $parentProperty = null): QueryPart {
        // simple cases
        $authQP = $this->repo->getMetadataAuthQuery();
        if ($mode === self::META_NONE) {
            return new QueryPart("SELECT * FROM metadata_view WHERE false");
        } elseif ($mode === self::META_RESOURCE) {
            return new QueryPart(
                "SELECT * FROM (SELECT * FROM metadata_view WHERE id = ?) mt",
                [$this->id]
            );
        } elseif ($mode === self::META_IDS) {
            $query = "SELECT id, property, type, lang, value FROM metadata WHERE id = ? AND property = ?";
            $param = [$this->id, $this->repo->getSchema()->label];
            return new QueryPart($query . $authQP->query, array_merge($param, $authQP->param));
        }
        // get_relatives_metadata() cases
        $getRelParam = match ($mode) {
            self::META_NEIGHBORS => [0, 0, 1, 1],
            self::META_RELATIVES => [999999, -999999, 1, 0],
            self::META_RELATIVES_ONLY => [999999, -999999, 0, 0],
            self::META_RELATIVES_REVERSE => [999999, -999999, 1, 1],
            self::META_PARENTS => [0, -999999, 1, 0],
            self::META_PARENTS_ONLY => [0, -999999, 0, 0],
            self::META_PARENTS_REVERSE => [0, -999999, 1, 1],
            default => RepoDb::parseMetadataReadMode($mode),
        };

        $param = array_merge([$this->id, $parentProperty], $getRelParam, $authQP->param);
        $query = "SELECT * FROM get_relatives_metadata(?::bigint, ?::text, ?::int, -?::int, ?::bool, ?::bool)";
        return new QueryPart($query . $authQP->query, $param);
    }
}
