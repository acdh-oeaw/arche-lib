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
use exception\Deleted;
use exception\NotFound;
use exception\AmbiguousMatch;

/**
 * Description of Repository
 *
 * @author zozlak
 */
class Repo {

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

        return new Repo($baseUrl, $schema, $headers, $options);
    }

    private $client;
    private $baseUrl;
    private $headers;
    private $schema;
    private $txId;

    public function __construct(string $baseUrl, Schema $schema,
                                Schema $headers, array $guzzleOptions = []) {
        $this->client  = new Client($guzzleOptions);
        $this->baseUrl = $baseUrl;
        $this->headers = $headers;
        $this->schema  = $schema;
    }

    public function createResource(Resource $metadata,
                                   BinaryPayload $payload = null): RepoResource {
        $req = new Request('post', $this->baseUrl);
        if ($payload !== null) {
            $req = $payload->attachTo($req);
        }
        $resp = $this->sendRequest($req);
        $uri  = $resp->getHeader('Location')[0];
        $res  = new RepoResource($uri, $this);
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

    public function getResourceById(string $id): RepoResource {
        return $this->getResourceByIds([$id]);
    }

    public function getResourceByIds(array $ids): RepoResource {
        $url     = $this->baseUrl . 'search';
        $headers = [
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $body    = [];
        foreach ($ids as $i) {
            $body[] = http_build_query([
                'property[]' => $this->schema->id,
                'value[]'    => $i,
            ]);
        }
        $req     = new Request('post', $url, $headers, implode('&', $body));
        $resp    = $this->sendRequest($req);
        $graph   = new Graph();
        $graph->parse($resp->getBody());
        $matches = $graph->resourcesMatching($this->schema->searchMatch);
        switch (count($matches)) {
            case 0:
                throw new NotFound();
            case 1;
                return new RepoResource($matches[0]->getUri(), $this);
            default:
                throw new AmbiguousMatch();
        }
    }

    public function getResourcesBySqlQuery(string $query,
                                           array $parameters = [],
                                           string $mode = RepoResource::META_RESOURCE): array {
        $headers = [
            'Accept'                                       => 'application/n-triples',
            'Content-Type'                                 => 'application/x-www-form-urlencoded',
            $this->getHeaderName('metadataReadMode')       => $mode,
            $this->getHeaderName('metadataParentProperty') => $this->schema->parent
        ];
        $body    = http_build_query(['sql' => $query, 'sqlParam' => $parameters]);
        $req     = new Request('post', $this->baseUrl . 'search', $headers, $body);
        $resp    = $this->sendRequest($req);
        return $this->parseSearchResponse($resp);
    }

    public function getResourcesBySearchTerms(array $searchTerms,
                                              string $mode = RepoResource::META_RESOURCE): array {
        $headers = [
            'Accept'                                       => 'application/n-triples',
            'Content-Type'                                 => 'application/x-www-form-urlencoded',
            $this->getHeaderName('metadataReadMode')       => $mode,
            $this->getHeaderName('metadataParentProperty') => $this->schema->parent
        ];
        $body    = [];
        foreach ($searchTerms as $i) {
            $body[] = $i->getFormData();
        }
        $body = implode('&', $body);
        $req  = new Request('post', $this->baseUrl . 'search', $headers, $body);

        $resp = $this->sendRequest($req);
        return $this->parseSearchResponse($resp);
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

    private function parseSearchResponse(Response $resp): array {
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
            $obj       = new RepoResource($i->getUri(), $this);
            $obj->setGraph($i);
            $objects[] = $obj;
        }
        return $objects;
    }

}
