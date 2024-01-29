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

use RuntimeException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\ResponseInterface;
use quickRdf\DataFactory as DF;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdfIo\NQuadsSerializer;
use quickRdfIo\Util as RdfIoUtil;
use termTemplates\QuadTemplate as QT;
use acdhOeaw\arche\lib\exception\RepoLibException;
use acdhOeaw\arche\lib\promise\ResponsePromise;

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
        $this->repo     = $repo;
        $this->repoInt  = $repo;
        $this->metadata = new DatasetNode(DF::namedNode($url));
    }

    /**
     * Returns repository resource binary content.
     * 
     * @return ResponseInterface PSR-7 response containing resource's binary content
     */
    public function getContent(): ResponseInterface {
        return $this->getContentAsync()->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * synchronous version of getContent()
     * 
     * @return ResponsePromise
     * @see getContent()
     */
    public function getContentAsync(): ResponsePromise {
        $request = new Request('get', $this->getUri());
        return $this->repo->sendRequestAsync($request);
    }

    /**
     * Updates repository resource binary content with a given payload.
     * 
     * @param BinaryPayload $content new content
     * @param string $readMode scope of the metadata returned by the repository
     *   - see the META_* constants defined by the RepoResourceInterface 
     * @param string $parentProperty RDF property to be used by the metadata
     *   read mode denoted by the $readMode parameter
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @return void
     */
    public function updateContent(BinaryPayload $content,
                                  string $readMode = self::META_RESOURCE,
                                  ?string $parentProperty = null,
                                  array $resourceProperties = [],
                                  array $relativesProperties = []): void {
        $this->updateContentAsync($content, $readMode, $parentProperty, $resourceProperties, $relativesProperties)->wait();
    }

    /**
     * Asynchronous version of updateContent()
     * 
     * @param BinaryPayload $content
     * @param string $readMode
     * @param string|null $parentProperty
     * @return PromiseInterface
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @see updateContent()
     */
    public function updateContentAsync(BinaryPayload $content,
                                       string $readMode = self::META_RESOURCE,
                                       ?string $parentProperty = null,
                                       array $resourceProperties = [],
                                       array $relativesProperties = []): PromiseInterface {
        $request = new Request('put', (string) $this->getUri());
        $request = $content->attachTo($request);
        $request = $this->withReadHeaders($request, $readMode, $parentProperty, $resourceProperties, $relativesProperties);
        $promise = $this->repo->sendRequestAsync($request);
        $promise = $promise->then(function (Response $resp): void {
            $this->parseMetadata($resp);
        });
        return $promise;
    }

    /**
     * Checks if the resource has the binary content.
     * 
     * @return bool
     */
    public function hasBinaryContent(): bool {
        $this->loadMetadata();
        return $this->metadata->any(new QT(predicate: $this->repo->getSchema()->binarySize));
    }

    /**
     * Saves the object metadata to the repository.
     * 
     * Local metadata are automatically updated with the metadata resulting from the update.
     * 
     * @param string $updateMode metadata update mode - one of `RepoResource::UPDATE_MERGE`,
     *   `RepoResource::UPDATE_ADD` and `RepoResource::UPDATE_OVERWRITE`
     * @param string $readMode scope of the metadata returned by the repository
     *   - see the META_* constants defined by the RepoResourceInterface 
     * @param string $parentProperty RDF property to be used by the metadata
     *   read mode denoted by the $readMode parameter
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @return void
     */
    public function updateMetadata(string $updateMode = self::UPDATE_MERGE,
                                   string $readMode = self::META_RESOURCE,
                                   ?string $parentProperty = null,
                                   array $resourceProperties = [],
                                   array $relativesProperties = []): void {
        $this->updateMetadataAsync($updateMode, $readMode, $parentProperty, $resourceProperties, $relativesProperties)?->wait();
    }

    /**
     * Asynchronous version of updateMetadata()
     * 
     * @param string $updateMode
     * @param string $readMode
     * @param string|null $parentProperty
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @return PromiseInterface|null
     * @see updateMetadata()
     */
    public function updateMetadataAsync(string $updateMode = self::UPDATE_MERGE,
                                        string $readMode = self::META_RESOURCE,
                                        ?string $parentProperty = null,
                                        array $resourceProperties = [],
                                        array $relativesProperties = []): ?PromiseInterface {
        if (!$this->metaSynced) {
            $headers = [
                'Content-Type'                                  => 'application/n-triples',
                $this->repo->getHeaderName('metadataWriteMode') => $updateMode,
            ];
            $body    = (new NQuadsSerializer())->serialize($this->metadata);
            $req     = new Request('patch', (string) $this->getUri() . '/metadata', $headers, $body);
            $req     = $this->withReadHeaders($req, $readMode, $parentProperty, $resourceProperties, $relativesProperties);
            $promise = $this->repo->sendRequestAsync($req);
            $promise = $promise->then(function (ResponseInterface $resp): void {
                $this->parseMetadata($resp);
                $this->metaSynced = true;
            });
            return $promise;
        }
        return null;
    }

    /**
     * Deletes the repository resource.
     * 
     * Returns an array of deleted resources' URIs.
     * @param bool $tombstone should tombstones be removed for deleted resources?
     * @param bool $references should references to deleted resources be removed
     *   from other resources?
     * @param string $recursiveProperty is present, deletion continues recursively
     *   to all resources pointing to the deleted one with this RDF property
     * @return array<string>
     */
    public function delete(bool $tombstone = false, bool $references = false,
                           string $recursiveProperty = ''): array {
        $result = $this->deleteAsync($tombstone, $references, $recursiveProperty)->wait();
        $g      = new Dataset();
        if (!is_array($result)) {
            $result = [$result];
        }
        $deleted = [];
        foreach ($result as $i) {
            foreach (RdfIoUtil::parse($i, new DF()) as $triple) {
                $deleted[$triple->getSubject()->getValue()] = '';
            }
        }
        return array_keys($deleted);
    }

    /**
     * Asynchronous version of delete()
     * 
     * @param bool $tombstone
     * @param bool $references
     * @param string $recursiveProperty
     * @return PromiseInterface
     * @see delete()
     */
    public function deleteAsync(bool $tombstone = false,
                                bool $references = false,
                                string $recursiveProperty = ''): PromiseInterface {
        $headers = [
            'Accept' => 'application/n-triples',
        ];
        if ($references) {
            $headers[$this->repo->getHeaderName('withReferences')] = '1';
        }
        if (!empty($recursiveProperty)) {
            $headers[$this->repo->getHeaderName('metadataParentProperty')] = $recursiveProperty;
        }
        $req            = new Request('delete', (string) $this->getUri(), $headers);
        $promise        = $this->repo->sendRequestAsync($req);
        $this->metadata = $this->metadata->withDataset(new Dataset());

        if ($tombstone) {
            $promise = $promise->then(function (ResponseInterface $resp): array {
                $respPromises = [];
                $deleted      = [];
                foreach (RdfIoUtil::parse($resp, new DF()) as $quad) {
                    $deleted[$quad->getSubject()->getValue()] = $quad->getSubject();
                }
                foreach ($deleted as $i) {
                    $req  = new Request('delete', $i->getValue() . '/tombstone');
                    $resp = $this->repo->sendRequest($req);
                    if ($resp->getStatusCode() === 204) {
                        // fake response of a delete without tombstone removal
                        $body = NQuadsSerializer::serializeQuad(DF::quad($i, $this->repo->getSchema()->id, $i));
                        $resp = $resp->
                            withBody(\GuzzleHttp\Psr7\Utils::streamFor($body))->
                            withHeader('Content-Type', 'application/n-triples');
                    }
                    $respPromises[] = $resp;
                }
                return $respPromises;
            });
        }
        return $promise;
    }

    /**
     * Loads current metadata from the repository.
     * 
     * @param bool $force enforce fetch from the repository 
     *   (when you want to make sure metadata are in line with ones in the repository 
     *   or e.g. reset them back to their current state in the repository)
     * @param string $mode scope of the metadata returned by the repository
     *   - see the META_* constants defined by the RepoResourceInterface 
     * @param string $parentProperty RDF property to be used by the metadata
     *   read mode denoted by the $mode parameter
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 ?string $parentProperty = null,
                                 array $resourceProperties = [],
                                 array $relativesProperties = []): void {
        $this->loadMetadataAsync($force, $mode, $parentProperty, $resourceProperties, $relativesProperties)?->wait();
    }

    /**
     * Asynchronous version of loadMetadata()
     * 
     * @param bool $force
     * @param string $mode
     * @param string|null $parentProperty
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @return PromiseInterface|null
     * @see loadMetadata()
     */
    public function loadMetadataAsync(bool $force = false,
                                      string $mode = self::META_RESOURCE,
                                      ?string $parentProperty = null,
                                      array $resourceProperties = [],
                                      array $relativesProperties = []): ?PromiseInterface {
        if (count($this->metadata) === 0 || $force) {
            $req     = new Request('get', (string) $this->getUri() . '/metadata');
            $req     = $this->withReadHeaders($req, $mode, $parentProperty, $resourceProperties, $relativesProperties);
            $promise = $this->repo->sendRequestAsync($req);
            $promise = $promise->then(function (ResponseInterface $resp): void {
                $this->parseMetadata($resp);
            });
            return $promise;
        }
        return null;
    }

    /**
     * Merges the current resource with the given one. See the corresponding 
     * [REST endpoint description](https://app.swaggerhub.com/apis/zozlak/arche/3.0#/default/put_merge__srcResourceId___targetResourceId_)
     * 
     * If this action succeeds, resource's URI changes to the targetResource's one.
     * 
     * @param string $targetResId
     * @param string $readMode scope of the metadata returned by the repository
     *   - see the META_* constants defined by the RepoResourceInterface 
     * @param string $parentProperty RDF property to be used by the metadata
     *   read mode denoted by the $readMode parameter
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @return void
     */
    public function merge(string $targetResId,
                          string $readMode = self::META_RESOURCE,
                          ?string $parentProperty = null,
                          array $resourceProperties = [],
                          array $relativesProperties = []): void {
        $this->mergeAsync($targetResId, $readMode, $parentProperty)->wait();
    }

    /**
     * Asynchronous version of merge()
     * 
     * @param string $targetResId
     * @param string $readMode
     * @param string|null $parentProperty
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @return PromiseInterface
     * @see merge()
     */
    public function mergeAsync(string $targetResId,
                               string $readMode = self::META_RESOURCE,
                               ?string $parentProperty = null,
                               array $resourceProperties = [],
                               array $relativesProperties = []): PromiseInterface {
        $baseUrl   = $this->repo->getBaseUrl();
        $srcId     = $this->getId();
        $targetRes = $this->repo->getResourceById($targetResId);
        $targetId  = substr($targetRes->getUri(), strlen($baseUrl));
        $request   = new Request('PUT', $baseUrl . "merge/$srcId/$targetId");
        $request   = $this->withReadHeaders($request, $readMode, $parentProperty, $resourceProperties, $relativesProperties);
        $promise   = $this->repo->sendRequestAsync($request);
        $promise   = $promise->then(function (ResponseInterface $resp) use ($targetRes): void {
            $this->metadata = $this->metadata->withNode(DF::namedNode($targetRes->getUri()));
            $this->parseMetadata($resp);
        });
        return $promise;
    }

    /**
     * Parses metadata fetched from the repository.
     * 
     * @param ResponseInterface $resp response to the metadata fetch HTTP request.
     * @return void
     */
    private function parseMetadata(ResponseInterface $resp): void {
        $graph = new Dataset();
        switch ($resp->getStatusCode()) {
            case 200:
            case 201:
                $graph->add(RdfIoUtil::parse($resp, new DF()));
                break;
            case 204:
                break;
            default:
                throw new RepoLibException("Invalid response status code: " . $resp->getStatusCode() . " with body: " . $resp->getBody());
        }
        $this->metadata   = $this->metadata->withDataset($graph);
        $this->metaSynced = true;
    }

    /**
     * 
     * @param Request $request
     * @param string $mode
     * @param string|null $parentProperty
     * @param array<string> $resourceProperties
     * @param array<string> $relativesProperties
     * @return Request
     */
    private function withReadHeaders(Request $request, string $mode,
                                     ?string $parentProperty,
                                     array $resourceProperties,
                                     array $relativesProperties): Request {
        $request = $request->
            withHeader('Accept', 'application/n-triples')->
            withHeader($this->repo->getHeaderName('metadataReadMode'), $mode)->
            withHeader($this->repo->getHeaderName('metadataParentProperty'), $parentProperty ?? $this->repo->getSchema()->parent->getValue());
        if (count($resourceProperties) > 0) {
            $request = $request->withHeader($this->repo->getHeaderName('resourceProperties'), implode(',', $resourceProperties));
        }
        if (count($relativesProperties) > 0) {
            $request = $request->withHeader($this->repo->getHeaderName('relativesProperties'), implode(',', $relativesProperties));
        }
        return $request;
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
