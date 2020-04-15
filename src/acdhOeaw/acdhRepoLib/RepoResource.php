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

use EasyRdf\Graph;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

/**
 * Description of RepoResource
 *
 * @author zozlak
 */
class RepoResource implements RepoResourceInterface {

    use RepoResourceTrait;

    const UPDATE_ADD       = 'add';
    const UPDATE_OVERWRITE = 'overwrite';
    const UPDATE_MERGE     = 'merge';
    const DELETE_STEP      = 1000;

    /**
     *
     * @var acdhOeaw\acdhRepoLib\Repo
     */
    private $repo;

    /**
     *
     * @var string
     */
    private $url;

    /**
     * Creates an object representing a repository resource.
     * 
     * @param string $url URL of the resource
     * @param \acdhOeaw\acdhRepoLib\RepoInterface $repo repository connection object
     */
    public function __construct(string $url, RepoInterface $repo) {
        $this->url  = $url;
        $this->repo = $repo;
    }

    /**
     * Returns repository resource binary content.
     * 
     * @return Response PSR-7 response containing resource's binary content
     */
    public function getContent(): Response {
        $request = new Request('get', $this->url);
        return $this->repo->sendRequest($request);
    }

    /**
     * Updates repository resource binary content with a given payload.
     * 
     * @param \acdhOeaw\acdhRepoLib\BinaryPayload $content new content
     * @return void
     */
    public function updateContent(BinaryPayload $content): void {
        $request = new Request('put', $this->url);
        $request = $content->attachTo($request);
        $this->repo->sendRequest($request);
        $this->loadMetadata(true);
    }

    /**
     * Checks if the resource has the binary content.
     * 
     * @return bool
     */
    public function hasBinaryContent(): bool {
        $this->loadMetadata();
        return (int) ((string) $this->metadata->getLiteral($this->repo->getSchema()->binarySize)) > 0;
    }

    /**
     * Saves the object metadata to the repository.
     * 
     * Local metadata are automatically updated with the metadata resulting from the update.
     * 
     * @param string $updateMode metadata update mode - one of `RepoResource::UPDATE_MERGE`,
     *   `RepoResource::UPDATE_ADD` and `RepoResource::UPDATE_OVERWRITE`
     * @param string $readMode scope of the metadata returned by the repository: 
     *   `RepoResourceInterface::META_RESOURCE` - only given resource metadata,
     *   `RepoResourceInterface::META_NEIGHBORS` - metadata of a given resource and all the resources pointed by its metadata,
     *   `RepoResourceInterface::META_RELATIVES` - metadata of a given resource and all resources recursively pointed to a given metadata property
     *      (see the `$parentProperty` parameter), both directly and in a reverse order (reverse in RDF terms)
     * @return void
     */
    public function updateMetadata(string $updateMode = self::UPDATE_MERGE,
                                   string $readMode = self::META_RESOURCE): void {
        if (!$this->metaSynced) {
            $updateModeHeader = $this->getRepo()->getHeaderName('metadataWriteMode');
            $readModeHeader   = $this->getRepo()->getHeaderName('metadataReadMode');
            $headers          = [
                'Content-Type'    => 'application/n-triples',
                'Accept'          => 'application/n-triples',
                $updateModeHeader => $updateMode,
                $readModeHeader   => $readMode,
            ];
            $body             = $this->metadata->getGraph()->serialise('application/n-triples');
            $req              = new Request('patch', $this->url . '/metadata', $headers, $body);
            $resp             = $this->repo->sendRequest($req);
            $this->parseMetadata($resp);
            $this->metaSynced = true;
        }
    }

    /**
     * Deletes the repository resource.
     * 
     * @param bool $tombstone should tombstones be removed for deleted resources?
     * @param bool $references should references to deleted resources be removed
     *   from other resources?
     * @return void
     */
    public function delete(bool $tombstone = false, bool $references = false): void {
        $req = new Request('delete', $this->getUri());
        $this->repo->sendRequest($req);

        if ($tombstone) {
            $req = new Request('delete', $this->getUri() . '/tombstone');
            $this->repo->sendRequest($req);
        }

        if ($references) {
            $query             = "SELECT id FROM relations WHERE target_id = ? ORDER BY id";
            $cfg               = new SearchConfig();
            $cfg->metadataMode = RepoResourceInterface::META_RESOURCE;
            $cfg->offset       = 0;
            $cfg->limit        = self::DELETE_STEP;
            do {
                $refRes = $this->repo->getResourcesBySqlQuery($query, [$this->getId()], $cfg);
                foreach ($refRes as $res) {
                    /* @var $res \acdhOeaw\acdhRepoLib\RepoResource */
                    $res->loadMetadata(false, self::META_RESOURCE);
                    $meta = $res->getMetadata();
                    foreach ($meta->propertyUris() as $p) {
                        $meta->deleteResource($p, $this->getUri());
                        if (null === $meta->getResource($p)) {
                            $meta->addResource($this->repo->getSchema()->delete, $p);
                        }
                    }
                    $res->setMetadata($meta);
                    $res->updateMetadata();
                }
                // don't increment offset - every query excludes resources with references which were already removed
            } while (count($refRes) > 1);
        }

        $this->metadata = null;
    }

    /**
     * Deletes the repository resource as well as all the resources pointing to
     * it with a given metadata property.
     * 
     * The deletion is performed recursively.
     * 
     * @param string $property property used for the recursive deletion
     * @param bool $tombstone should tombstones be removed for deleted resources?
     * @param bool $references should references to deleted resources be removed
     *   from other resources?
     * @return void
     */
    public function deleteRecursively(string $property, bool $tombstone = false,
                                      bool $references = false): void {
        $query             = "SELECT id FROM get_relatives(?, ?, 999999, 0) ORDER BY n DESC, id";
        $param             = [$this->getId(), $property];
        $cfg               = new SearchConfig();
        $cfg->metadataMode = RepoResourceInterface::META_RESOURCE;
        $cfg->offset       = 0;
        $cfg->limit        = self::DELETE_STEP;
        do {
            $resources = $this->repo->getResourcesBySqlQuery($query, $param, $cfg);
            foreach ($resources as $res) {
                /* @var $res \acdhOeaw\acdhRepoLib\RepoResource */
                $res->delete($tombstone, $references);
            }
            // don't increment offset - every query excludes resources which were already removed
        } while (count($resources) > 1);
    }

    /**
     * Loads current metadata from the repository.
     * 
     * @param bool $force enforce fetch from the repository 
     *   (when you want to make sure metadata are in line with ones in the repository 
     *   or e.g. reset them back to their current state in the repository)
     * @param string $mode scope of the metadata returned by the repository: 
     *   `RepoResource::META_RESOURCE` - only given resource metadata,
     *   `RepoResource::META_NEIGHBORS` - metadata of a given resource and all the resources pointed by its metadata,
     *   `RepoResource::META_RELATIVES` - metadata of a given resource and all resources recursively pointed to a given metadata property
     *      (see the `$parentProperty` parameter), both directly and in a reverse order (reverse in RDF terms)
     * @param string $parentProperty RDF property name used to find related resources in the `RepoResource::META_RELATIVES` mode
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 ?string $parentProperty = null): void {
        if (!is_object($this->metadata) || $force) {
            $headers = [
                'Accept'                                             => 'application/n-triples',
                $this->repo->getHeaderName('metadataReadMode')       => $mode,
                $this->repo->getHeaderName('metadataParentProperty') => $parentProperty ?? $this->repo->getSchema()->parent,
            ];
            $req     = new Request('get', $this->url . '/metadata', $headers);
            $resp    = $this->repo->sendRequest($req);
            $this->parseMetadata($resp);
        }
    }

    /**
     * Parses metadata fetched from the repository.
     * 
     * @param \GuzzleHttp\Psr7\Response $resp response to the metadata fetch HTTP request.
     * @return void
     */
    private function parseMetadata(Response $resp): void {
        $format           = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
        $graph            = new Graph();
        $graph->parse($resp->getBody(), $format);
        $this->metadata   = $graph->resource($this->url);
        $this->metaSynced = true;
    }

    /**
     * Returns an internal repository resource identifier.
     * 
     * @return int
     */
    protected function getId(): int {
        return (int) substr($this->getUri(), strlen($this->repo->getBaseUrl()));
    }

}
