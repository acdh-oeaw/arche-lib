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

use acdhOeaw\acdhRepoLib\exception\RepoLibException;

/**
 * Provides a read-only access to the repository resource's metadata.
 * 
 * @author zozlak
 */
class RepoResourceDb implements RepoResourceInterface {

    use RepoResourceTrait;

    /**
     *
     * @var int
     */
    private $id;

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\RepoDb
     */
    private $repo;

    /**
     * Creates an object representing a repository resource.
     * 
     * @param string $urlOrId either a resource URL or just the numeric id
     * @param \acdhOeaw\acdhRepoLib\RepoInterface $repo repository connection object
     */
    public function __construct(string $urlOrId, RepoInterface $repo) {
        if (!$repo instanceof RepoDb) {
            throw new RepoLibException('The RepoResourceDb object can be created only with a RepoDb repository connection handle');
        }
        if (!is_numeric($urlOrId)) {
            $urlOrId = preg_replace('/^.*[^0-9]/', '', $urlOrId);
        }
        $this->id   = (int) $urlOrId;
        $this->repo = $repo;
        $this->url  = $this->repo->getBaseUrl() . $this->id;
    }

    /**
     * Loads current metadata from the repository.
     * 
     * @param bool $force enforce fetch from the repository 
     *   (when you want to make sure metadata are in line with ones in the repository 
     *   or e.g. reset them back to their current state in the repository)
     * @param string $mode scope of the metadata returned by the repository: 
     *   `RepoResourceInterface::META_RESOURCE` - only given resource metadata,
     *   `RepoResourceInterface::META_NEIGHBORS` - metadata of a given resource and all the resources pointed by its metadata,
     *   `RepoResourceInterface::META_RELATIVES` - metadata of a given resource and all resources recursively pointed to a given metadata property
     *      (see the `$parentProperty` parameter), both directly and in a reverse order (reverse in RDF terms)
     * @param string $parentProperty RDF property name used to find related resources in the `RepoResource::META_RELATIVES` mode
     * @return void
     * @throws RepoLibException
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 ?string $parentProperty = null): void {
        if (!$force && $this->metadata !== null) {
            return;
        }
        switch ($mode) {
            case self::META_RESOURCE:
                $query = "SELECT * FROM (SELECT * FROM metadata_view WHERE id = ?) mt";
                $param = [$this->id];
                break;
            case self::META_NEIGHBORS:
                $query = "SELECT * FROM get_neighbors_metadata(?, ?)";
                $param = [$this->id, $parentProperty];
                break;
            case self::META_RELATIVES:
                $query = "SELECT * FROM get_relatives_metadata(?, ?)";
                $param = [$this->id, $parentProperty];
                break;
            default:
                throw new RepoLibException('Bad metadata mode ' . $mode, 400);
        }
        $authQP         = $this->repo->getMetadataAuthQuery();
        $query          = $this->repo->runQuery($query . $authQP->query, array_merge($param, $authQP->param));
        $graph          = $this->repo->parsePdoStatement($query);
        $this->metadata = $graph->resource($this->getUri());
    }

}
