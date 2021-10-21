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

use EasyRdf\Graph;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use acdhOeaw\arche\lib\exception\RepoLibException;

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
     * Creates a repository resource object from the PSR-7 response object
     * returning the metadata.
     * 
     * @param Repo $repo connection object
     * @param ResponseInterface $response PSR-7 repository response object
     * @param ?string $uri resource URI (if not provided, a Location HTTP header
     *   from the response will be used)
     * @return RepoResource
     */
    static public function factory(Repo $repo, ResponseInterface $response,
                                   ?string $uri = null): RepoResource {

        $uri   = $uri ?? $response->getHeader('Location')[0];
        /* @var $res \acdhOeaw\arche\lib\RepoResource */
        $class = get_called_class();
        $res   = new $class($uri, $repo);

        if (count($response->getHeader('Content-Type')) > 0) {
            $res->parseMetadata($response);
        }

        return $res;
    }

    private Repo $repo;

    /**
     * Creates an object representing a repository resource.
     * 
     * @param string $url URL of the resource
     * @param RepoInterface $repo repository connection object
     */
    public function __construct(string $url, RepoInterface $repo) {
        if (!$repo instanceof Repo) {
            throw new RepoLibException('The RepoResource object can be created only with a Repo repository connection handle');
        }
        $this->url     = $url;
        $this->repo    = $repo;
        $this->repoInt = $repo;
    }

    /**
     * Returns repository resource binary content.
     * 
     * @return ResponseInterface PSR-7 response containing resource's binary content
     */
    public function getContent(): ResponseInterface {
        $request = new Request('get', $this->url);
        return $this->repo->sendRequest($request);
    }

    /**
     * Updates repository resource binary content with a given payload.
     * 
     * @param BinaryPayload $content new content
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
        return (int) ((string) $this->metadata?->getLiteral($this->repo->getSchema()->binarySize)) > 0;
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
            $updateModeHeader = $this->repo->getHeaderName('metadataWriteMode');
            $readModeHeader   = $this->repo->getHeaderName('metadataReadMode');
            $headers          = [
                'Content-Type'    => 'application/n-triples',
                'Accept'          => 'application/n-triples',
                $updateModeHeader => $updateMode,
                $readModeHeader   => $readMode,
            ];
            $body             = $this->metadata?->getGraph()->serialise('application/n-triples');
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
     * @param string $recursiveProperty is present, deletion continues recursively
     *   to all resources pointing to the deleted one with this RDF property
     * @return void
     */
    public function delete(bool $tombstone = false, bool $references = false,
                           string $recursiveProperty = ''): void {
        $headers = [
            'Accept' => 'application/n-triples',
        ];
        if ($references) {
            $headers[$this->repo->getHeaderName('withReferences')] = '1';
        }
        if (!empty($recursiveProperty)) {
            $headers[$this->repo->getHeaderName('metadataParentProperty')] = $recursiveProperty;
        }
        $req  = new Request('delete', $this->getUri(), $headers);
        $resp = $this->repo->sendRequest($req);

        if ($tombstone) {
            $format = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];

            $graph = new Graph();
            $graph->parse((string) $resp->getBody(), $format);
            foreach ($graph->resources() as $i) {
                if (count($i->propertyUris()) > 0) {
                    $req = new Request('delete', $i->getUri() . '/tombstone');
                    $this->repo->sendRequest($req);
                }
            }
        }
        $this->metadata = null;
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
        if ($this->metadata === null || $force) {
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
     * Merges the current resource with the given one. See the corresponding 
     * [REST endpoint description](https://app.swaggerhub.com/apis/zozlak/arche/3.0#/default/put_merge__srcResourceId___targetResourceId_)
     * 
     * If this action succeeds, resource's URI changes to the targetResource's one.
     * 
     * @param string $targetResId
     * @return void
     */
    public function merge(string $targetResId): void {
        $baseUrl   = $this->repo->getBaseUrl();
        $srcId     = $this->getId();
        $targetRes = $this->repo->getResourceById($targetResId);
        $targetId  = substr($targetRes->getUri(), strlen($baseUrl));
        $request   = new Request('PUT', $baseUrl . "merge/$srcId/$targetId");
        $resp      = $this->repo->sendRequest($request);
        $this->url = $targetRes->getUri();
        $this->parseMetadata($resp);
    }

    /**
     * Parses metadata fetched from the repository.
     * 
     * @param ResponseInterface $resp response to the metadata fetch HTTP request.
     * @return void
     */
    private function parseMetadata(ResponseInterface $resp): void {
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
