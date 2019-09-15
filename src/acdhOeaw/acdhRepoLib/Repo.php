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
use EasyRdf\Resource;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\RequestException;
use acdhOeaw\acdhRepoLib\exception\Deleted;
use acdhOeaw\acdhRepoLib\exception\NotFound;
use acdhOeaw\acdhRepoLib\exception\AmbiguousMatch;

/**
 * A repository connection class.
 *
 * @author zozlak
 */
class Repo {

    /**
     * A class used to instantiate objects representing repository resources.
     * 
     * To be used by external libraries extending the RepoResource class funcionality provided by this library.
     * 
     * @var string
     */
    static public $resourceClass = '\acdhOeaw\acdhRepoLib\RepoResource';

    /**
     * Creates a repository object instance from a given configuration file.
     * 
     * Automatically parses required config properties and passes them to the Repo object constructor.
     * 
     * @param string $configFile a path to the YAML config file
     * @return \acdhOeaw\acdhRepoLib\Repo
     */
    static public function factory(string $configFile): Repo {
        $config = json_decode(json_encode(yaml_parse_file($configFile)));

        $baseUrl            = $config->rest->urlBase . $config->rest->pathBase;
        $schema             = new Schema($config->schema);
        $headers            = new Schema($config->rest->headers);
        $options            = [];
        $options['headers'] = (array) $config->auth->httpHeader ?? [];
        if (!empty($config->auth->httpBasic->user ?? '')) {
            $options['auth'] = [$config->auth->httpBasic->user, $config->auth->httpBasic->password ?? ''];
        }
        if (($config->rest->verifyCert ?? true) === false) {
            $options['verify'] = false;
        }

        return new Repo($baseUrl, $schema, $headers, $options);
    }

    /**
     * The Guzzle client object used to send HTTP requests
     * 
     * @var \GuzzleHttp\Client
     */
    private $client;

    /**
     * Repository REST API base URL
     * 
     * @var string
     */
    private $baseUrl;

    /**
     * An object providing mappings of repository REST API parameters to HTTP headers used by a given repository instance.
     * 
     * @var \acdhOeaw\acdhRepoLib\Schema
     */
    private $headers;

    /**
     * An object providing mappings of repository concepts to RDF properties used to denote them by a given repository instance.
     * 
     * @var \acdhOeaw\acdhRepoLib\Schema
     */
    private $schema;

    /**
     * Current transaction id
     * 
     * @var string
     */
    private $txId;

    /**
     * Creates an repository connection object.
     * 
     * @param string $baseUrl repository REST API base URL
     * @param \acdhOeaw\acdhRepoLib\Schema $schema mappings between repository concepts and RDF properties used to denote them by a given repository instance
     * @param \acdhOeaw\acdhRepoLib\Schema $headers mappings between repository REST API parameters and HTTP headers used to pass them to a given repository instance
     * @param array $guzzleOptions Guzzle HTTP client connection options to be used by all requests to the repository REST API (e.g. credentials)
     */
    public function __construct(string $baseUrl, Schema $schema,
                                Schema $headers, array $guzzleOptions = []) {
        $this->client  = new Client($guzzleOptions);
        $this->baseUrl = $baseUrl;
        $this->headers = $headers;
        $this->schema  = $schema;
    }

    /**
     * 
     * @param Resource $metadata
     * @param \acdhOeaw\acdhRepoLib\BinaryPayload $payload
     * @param string $class
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     */
    public function createResource(Resource $metadata,
                                   BinaryPayload $payload = null,
                                   string $class = null): RepoResource {
        $req = new Request('post', $this->baseUrl);
        if ($payload !== null) {
            $req = $payload->attachTo($req);
        }
        $resp  = $this->sendRequest($req);
        $uri   = $resp->getHeader('Location')[0];
        $class = $class ?? self::$resourceClass;
        $res   = new $class($uri, $this);
        $res->setMetadata($metadata);
        $res->updateMetadata();
        return $res;
    }

    public function sendRequest(Request $request): Response {
        if (!empty($this->txId)) {
            $request = $request->withHeader($this->getHeaderName('transactionId'), $this->txId);
        }
        try {
            $response = $this->client->send($request);
        } catch (RequestException $e) {
            switch ($e->getCode()) {
                case 410:
                    throw new Deleted();
                case 404:
                    throw new NotFound();
                default:
                    throw $e;
            }
        }
        return $response;
    }

    public function getResourceById(string $id, string $class = null): RepoResource {
        return $this->getResourceByIds([$id], $class);
    }

    public function getResourceByIds(array $ids, string $class = null): RepoResource {
        $url          = $this->baseUrl . 'search';
        $headers      = [
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $placeholders = substr(str_repeat('?, ', count($ids)), 0, -2);
        $query        = "SELECT DISTINCT id FROM identifiers WHERE ids IN ($placeholders)";
        $body         = http_build_query([
            'sql'      => $query,
            'sqlParam' => $ids,
        ]);
        $req          = new Request('post', $url, $headers, $body);
        $resp         = $this->sendRequest($req);
        $format       = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
        $graph        = new Graph();
        $graph->parse($resp->getBody(), $format);
        $matches      = $graph->resourcesMatching($this->schema->searchMatch);
        switch (count($matches)) {
            case 0:
                throw new NotFound();
            case 1;
                $class = $class ?? self::$resourceClass;
                return new $class($matches[0]->getUri(), $this);
            default:
                throw new AmbiguousMatch();
        }
    }

    public function getResourcesBySqlQuery(string $query,
                                           array $parameters = [],
                                           string $mode = RepoResource::META_RESOURCE,
                                           string $parentProperty = null,
                                           string $class = null): array {
        $headers = [
            'Accept'                                       => 'application/n-triples',
            'Content-Type'                                 => 'application/x-www-form-urlencoded',
            $this->getHeaderName('metadataReadMode')       => $mode,
            $this->getHeaderName('metadataParentProperty') => $parentProperty ?? $this->schema->parent
        ];
        $body    = http_build_query(['sql' => $query, 'sqlParam' => $parameters]);
        $req     = new Request('post', $this->baseUrl . 'search', $headers, $body);
        $resp    = $this->sendRequest($req);
        return $this->parseSearchResponse($resp, $class);
    }

    /**
     * Returns repository resources matching all provided terms.
     * @param array $searchTerms an array of SearchTerm class objects describing the search filters
     * @param string $mode scope of the metadata returned by the repository 
     * @param string $class
     * @return array
     */
    public function getResourcesBySearchTerms(array $searchTerms,
                                              string $mode = RepoResource::META_RESOURCE,
                                              string $parentProperty = null,
                                              string $class = null): array {
        $headers = [
            'Accept'                                       => 'application/n-triples',
            'Content-Type'                                 => 'application/x-www-form-urlencoded',
            $this->getHeaderName('metadataReadMode')       => $mode,
            $this->getHeaderName('metadataParentProperty') => $parentProperty ?? $this->schema->parent
        ];
        $body    = [];
        foreach ($searchTerms as $i) {
            $body[] = $i->getFormData();
        }
        $body = implode('&', $body);
        $req  = new Request('post', $this->baseUrl . 'search', $headers, $body);

        $resp = $this->sendRequest($req);
        return $this->parseSearchResponse($resp, $class);
    }

    public function begin(): void {
        $req        = new Request('post', $this->baseUrl . 'transaction');
        $resp       = $this->sendRequest($req);
        $this->txId = $resp->getHeader($this->getHeaderName('transactionId'))[0];
    }

    public function rollback(): void {
        if (!empty($this->txId)) {
            $headers    = [$this->getHeaderName('transactionId') => $this->txId];
            $req        = new Request('delete', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
            $this->txId = null;
        }
    }

    public function commit(): void {
        if (!empty($this->txId)) {
            $headers    = [$this->getHeaderName('transactionId') => $this->txId];
            $req        = new Request('put', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
            $this->txId = null;
        }
    }

    public function prolong(): void {
        if (!empty($this->txId)) {
            $headers = [$this->getHeaderName('transactionId') => $this->txId];
            $req     = new Request('patch', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
        }
    }

    public function inTransaction(): bool {
        return !empty($this->txId);
    }

    public function getSchema(): Schema {
        return $this->schema;
    }

    public function getHeaderName(string $purpose): ?string {
        return $this->headers->$purpose ?? null;
    }

    public function getBaseUrl(): string {
        return $this->baseUrl;
    }

    private function parseSearchResponse(Response $resp, string $class = null): array {
        $class = $class ?? self::$resourceClass;

        $graph = new Graph();
        $body  = $resp->getBody();
        if (empty($body)) {
            return [];
        }
        $format = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
        $graph->parse($body, $format);

        $resources = $graph->resourcesMatching($this->schema->searchMatch);
        $objects   = [];
        foreach ($resources as $i) {
            $obj       = new $class($i->getUri(), $this);
            $obj->setGraph($i);
            $objects[] = $obj;
        }
        return $objects;
    }

}
