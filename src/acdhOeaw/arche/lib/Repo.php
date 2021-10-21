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
use EasyRdf\Graph;
use EasyRdf\Resource;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Promise\RejectedPromise;
use Psr\Http\Message\ResponseInterface;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\promise\GeneratorPromise;
use acdhOeaw\arche\lib\promise\ResponsePromise;
use acdhOeaw\arche\lib\promise\RepoResourcePromise;
use acdhOeaw\arche\lib\SearchTerm;
use function GuzzleHttp\json_decode;

/**
 * A repository connection class.
 *
 * @author zozlak
 */
class Repo implements RepoInterface {

    const REJECT_SKIP    = 1;
    const REJECT_FAIL    = 2;
    const REJECT_INCLUDE = 3;
    use RepoTrait;

    /**
     * A class used to instantiate objects representing repository resources.
     * 
     * To be used by external libraries extending the RepoResource class funcionality provided by this library.
     * 
     * @var string
     */
    static public $resourceClass = RepoResource::class;

    /**
     * Creates a repository object instance from a given configuration file.
     * 
     * Automatically parses required config properties and passes them to the Repo object constructor.
     * 
     * @param string $configFile a path to the YAML config file
     * @return Repo
     */
    static public function factory(string $configFile): Repo {
        $config = Config::fromYaml($configFile);

        $baseUrl            = $config->rest->urlBase . $config->rest->pathBase;
        $schema             = new Schema($config->schema);
        $headers            = new Schema($config->rest->headers);
        $options            = [];
        $options['headers'] = (array) ($config->auth->httpHeader ?? []);
        if (!empty($config->auth->httpBasic->user ?? '')) {
            $options['auth'] = [$config->auth->httpBasic->user, $config->auth->httpBasic->password ?? ''];
        }
        if (($config->rest->verifyCert ?? true) === false) {
            $options['verify'] = false;
        }

        return new Repo($baseUrl, $schema, $headers, $options);
    }

    static public function factoryInteractive(string $cfgPath = '.'): self {
        if (!file_exists($cfgPath) || !is_file($cfgPath)) {
            while (file_exists($cfgPath) && !file_exists($cfgPath . '/config.yaml')) {
                $cfgPath .= '/..';
            }
            $cfgPath .= '/config.yaml';
            if (!file_exists($cfgPath) || !is_file($cfgPath)) {
                exit("No config.yaml found.\n");
            }
        }
        echo "Configuration found at $cfgPath\n";
        $cfg = Config::fromYaml($cfgPath);

        if (isset($cfg->repositories)) {
            echo "\nWhat's the repository you want to ingest to? (type a number)\n";
            foreach ($cfg->repositories as $k => $v) {
                echo ($k + 1) . "\t" . $v->urlBase . $v->pathBase . "\n";
            }
            $line     = ((int) trim((string) fgets(STDIN))) - 1;
            $urlBase  = $cfg->repositories[$line]->urlBase ?? '';
            $pathBase = $cfg->repositories[$line]->pathBase ?? '';
        } else {
            $urlBase  = $cfg->rest->urlBase ?? '';
            $pathBase = $cfg->rest->pathBase ?? '';
        }
        if (empty($urlBase . $pathBase)) {
            exit("Repository URL not set. Please reaview your config.yaml.\n");
        }
        echo "\nIs repository URL $urlBase$pathBase correct? (type 'yes' to continue)\n";
        $line = trim((string) fgets(STDIN));
        if ($line !== 'yes') {
            exit("Wrong repository URL\n");
        }
        $cfg->rest->urlBase  = $urlBase;
        $cfg->rest->pathBase = $pathBase;

        $user = $cfg->auth->httpBasic->user ?? '';
        if (empty($user)) {
            echo "\nWhat's your login? (login not set in the config.yaml)\n";
            $user = trim((string) fgets(STDIN));
        }

        echo "\nWhat's your password?\n";
        system('stty -echo');
        $pswd = trim((string) fgets(STDIN));
        system('stty echo');

        $cfg->auth = new Config((object) ['httpBasic' => ['user' => $user, 'password' => $pswd]]);
        $tmpfile   = (string) tempnam('/tmp', '');
        file_put_contents($tmpfile, $cfg->asYaml());
        try {
            $repo = Repo::factory($tmpfile);
        } finally {
            unlink($tmpfile);
        }
        return $repo;
    }

    /**
     * Creates a Repo instance from any URL resolving to a repository resource.
     * 
     * It's not very fast but requires a zero config.
     * 
     * @param string $url
     * @param array<mixed> $guzzleOptions
     * @param string $realUrl if provided, the final resource URL will be stored
     *   in this variable.
     * @param string $metaReadModeHeader header used by the repository to denote
     *   the metadata read mode. Providing this parameter will make the resolution
     *   faster.
     * @return self
     */
    static public function factoryFromUrl(string $url,
                                          array $guzzleOptions = [],
                                          string &$realUrl = null,
                                          string $metaReadModeHeader = null): self {
        $resolveOptions                    = $guzzleOptions;
        $resolveOptions['http_errors']     = false;
        $resolveOptions['allow_redirects'] = ['max' => 10, 'strict' => true, 'track_redirects' => true];
        if (!empty($metaReadModeHeader)) {
            $resolveOptions['headers'] = array_merge(
                $resolveOptions['headers'] ?? [],
                [$metaReadModeHeader => RepoResourceInterface::META_RESOURCE]
            );
        }

        $client    = new Client($resolveOptions);
        $resp      = $client->send(new Request('HEAD', $url));
        $redirects = array_merge([$url], $resp->getHeader('X-Guzzle-Redirect-History'));
        $realUrl   = (string) array_pop($redirects);
        $realUrl   = (string) preg_replace('|/metadata$|', '', $realUrl);

        $baseUrl = preg_replace('@([0-9]+|describe/?)$@', '', $realUrl) ?? '';
        $resp    = $client->send(new Request('GET', "$baseUrl/describe", ['Accept' => 'application/json']));
        if ($resp->getStatusCode() !== 200) {
            throw new NotFound("Provided URL doesn't resolve to an ARCHE repository", 404);
        }
        $config  = new Config((object) json_decode((string) $resp->getBody()));
        $schema  = new Schema($config->schema);
        $headers = new Schema($config->rest->headers);
        return new Repo($baseUrl, $schema, $headers, $guzzleOptions);
    }

    /**
     * The Guzzle client object used to send HTTP requests
     * 
     * @var \GuzzleHttp\Client
     */
    private $client;

    /**
     * Current transaction id
     * 
     * @var ?string
     */
    private $txId;

    /**
     * Creates an repository connection object.
     * 
     * @param string $baseUrl repository REST API base URL
     * @param Schema $schema mappings between repository 
     *   concepts and RDF properties used to denote them by a given repository instance
     * @param Schema $headers mappings between repository 
     *   REST API parameters and HTTP headers used to pass them to a given repository instance
     * @param array<mixed> $guzzleOptions Guzzle HTTP client connection options to be used 
     *   by all requests to the repository REST API (e.g. credentials)
     */
    public function __construct(string $baseUrl, Schema $schema,
                                Schema $headers, array $guzzleOptions = []) {
        $this->client  = new Client($guzzleOptions);
        $this->baseUrl = $baseUrl;
        $this->headers = $headers;
        $this->schema  = $schema;
    }

    /**
     * Creates a repository resource.
     * 
     * @param Resource $metadata resource metadata
     * @param BinaryPayload $payload resource binary payload (can be null)
     * @param string $class an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @return RepoResource
     */
    public function createResource(Resource $metadata,
                                   BinaryPayload $payload = null,
                                   string $class = null): RepoResource {
        return $this->createResourceAsync($metadata, $payload, $class)->wait();
    }

    public function createResourceAsync(Resource $metadata,
                                        BinaryPayload $payload = null,
                                        string $class = null): RepoResourcePromise {
        $readModeHeader = $this->getHeaderName('metadataReadMode');
        $headers        = [
            'Content-Type'  => 'application/n-triples',
            'Accept'        => 'application/n-triples',
            $readModeHeader => RepoResource::META_RESOURCE,
        ];
        $graph          = new Graph();
        $metadata       = $body           = $metadata->copy([], '/^$/', $this->baseUrl, $graph);
        $body           = $graph->serialise('application/n-triples');
        $req            = new Request('post', $this->baseUrl . 'metadata', $headers, $body);
        $promise        = $this->sendRequestAsync($req)->then(function (ResponseInterface $resp) use ($payload,
                                                                                                      $class): RepoResource | RepoResourcePromise {
            $class = $class ?? self::$resourceClass;
            $res   = $class::factory($this, $resp);

            if ($payload !== null) {
                $promise = $res->updateContentAsync($payload)->then(function ()use ($res): RepoResource {
                    return $res;
                });
                return new RepoResourcePromise($promise);
            }
            return $res;
        });
        return new RepoResourcePromise($promise);
    }

    /**
     * Sends an HTTP request to the repository.
     * 
     * A low-level repository API method.
     * 
     * Handles most common errors which can be returned by the repository.
     * 
     * @param Request $request a PSR-7 HTTP request
     * @return ResponseInterface
     * @throws Deleted
     * @throws NotFound
     * @throws RequestException
     */
    public function sendRequest(Request $request): ResponseInterface {
        return $this->sendRequestAsync($request)->wait();
    }

    public function sendRequestAsync(Request $request): ResponsePromise {
        if (!empty($this->txId)) {
            $request = $request->withHeader($this->getHeaderName('transactionId'), $this->txId);
        }
        $promise = $this->client->sendAsync($request)->otherwise(
            function (RequestException $e) {
                switch ($e->getCode()) {
                    case 410:
                        return new RejectedPromise(new Deleted());
                    case 404:
                        return new RejectedPromise(new NotFound());
                    default:
                        return new RejectedPromise($e);
                }
            });
        return new ResponsePromise($promise);
    }

    /**
     * A wrapper function for parallel repository requests execution.
     * 
     * It calls $func for every element of $iter to generate a set of promises,
     * executes all of them and returns their results.
     * 
     * @param iterable<mixed> $iter collection of values to iterate over.
     * @param callable $func function to apply to each $iter element with
     *   signature `f(mixed $iterElement, Repo $thisRepoObject): GuzzleHttp\Promise\PromiseInterface`.
     * @param int $concurrency number of promises executed in parallel
     * @param int $rejectAction what to do with rejected promises - one of 
     *   Repo::REJECT_SKIP (skip the silently), Repo::REJECT_FAIL (throw an error)
     *   and Repo::REJECT_INCLUDE (include the rejection value in the results).
     * @return array<mixed>
     */
    public function map(iterable $iter, callable $func, int $concurrency = 1,
                        int $rejectAction = self::REJECT_SKIP): array {
        $promiseIterator = function ($i, $f) {
            foreach ($i as $j) {
                yield $f($j, $this);
            }
        };
        $results   = [];
        $queue     = new \GuzzleHttp\Promise\EachPromise(
            $promiseIterator($iter, $func),
                             [
            'concurrency' => $concurrency,
            'fulfilled'   => function ($x, $i) use (&$results) {
                $results[$i] = $x;
            },
            'rejected' => function ($x, $i) use (&$results, $rejectAction) {
                switch ($rejectAction) {
                    case self::REJECT_FAIL:
                        throw $x instanceof \Exception ? $x : new \RuntimeException($x);
                    case self::REJECT_INCLUDE:
                        $results[$i] = $x;
                        break;
                    case self::REJECT_SKIP:
                        break;
                    default:
                        throw new \RuntimeException("Unknown rejectAction");
                }
            },
            ]
        );
        $queue->promise()->wait();
        return $results;
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
     * @return RepoResource
     * @throws NotFound
     * @throws AmbiguousMatch
     */
    public function getResourceByIds(array $ids, string $class = null): RepoResource {
        return $this->getResourceByIdsAsync($ids, $class)->wait();
    }

    public function getResourceByIdAsync(string $id, string $class = null): RepoResourcePromise {
        return $this->getResourceByIdsAsync([$id], $class);
    }

    /**
     * 
     * @param array<string> $ids
     * @param string $class
     * @return RepoResourcePromise
     */
    public function getResourceByIdsAsync(array $ids, string $class = null): RepoResourcePromise {
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
        $promise      = $this->sendRequestAsync($req)->then(function (ResponseInterface $resp) use ($class) {
            $format  = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
            $graph   = new Graph();
            $graph->parse($resp->getBody(), $format);
            $matches = $graph->resourcesMatching($this->schema->searchMatch);
            switch (count($matches)) {
                case 0:
                    return new RejectedPromise(new NotFound());
                case 1;
                    $class = $class ?? self::$resourceClass;
                    return new $class($matches[0]->getUri(), $this);
                default:
                    $uris  = implode(', ', array_map(function ($x) {
                            return $x->getUri();
                        }, $matches));
                    return new RejectedPromise(new AmbiguousMatch("Many resources match the search: $uris"));
            }
        });
        return new RepoResourcePromise($promise);
    }

    /**
     * Performs a search
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return Generator<RepoResource>
     */
    public function getResourcesBySqlQuery(string $query, array $parameters,
                                           SearchConfig $config): Generator {
        return $this->getResourcesBySqlQueryAsync($query, $parameters, $config)->wait();
    }

    /**
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return GeneratorPromise
     */
    public function getResourcesBySqlQueryAsync(string $query,
                                                array $parameters,
                                                SearchConfig $config): GeneratorPromise {
        $headers = [
            'Accept'       => 'application/n-triples',
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $headers = array_merge($headers, $config->getHeaders($this));
        $body    = array_merge(
            ['sql' => $query, 'sqlParam' => $parameters],
            $config->toArray()
        );
        $body    = http_build_query($body);
        $req     = new Request('post', $this->baseUrl . 'search', $headers, $body);
        $promise = $this->sendRequestAsync($req)->then(function (ResponseInterface $resp) use ($config): Generator {
            yield from $this->parseSearchResponse($resp, $config);
        });
        return new GeneratorPromise($promise);
    }

    /**
     * Returns repository resources matching all provided search terms.
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return Generator<RepoResource>
     */
    public function getResourcesBySearchTerms(array $searchTerms,
                                              SearchConfig $config): Generator {
        return $this->getResourcesBySearchTermsAsync($searchTerms, $config)->wait();
    }

    /**
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return GeneratorPromise
     */
    public function getResourcesBySearchTermsAsync(array $searchTerms,
                                                   SearchConfig $config): GeneratorPromise {
        $headers = [
            'Accept'       => 'application/n-triples',
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $headers = array_merge($headers, $config->getHeaders($this));
        $body    = [];
        foreach ($searchTerms as $i) {
            $body[] = $i->getFormData();
        }
        $body = implode('&', $body);
        $body .= (!empty($body) ? '&' : '') . $config->toQuery();
        $req  = new Request('post', $this->baseUrl . 'search', $headers, $body);

        $promise = $this->sendRequestAsync($req)->then(function (ResponseInterface $resp) use ($config): Generator {
            yield from $this->parseSearchResponse($resp, $config);
        });
        return new GeneratorPromise($promise);
    }

    /**
     * Begins a transaction.
     * 
     * All data modifications must be performed within a transaction.
     * 
     * @return void
     * @see rollback()
     * @see commit()
     */
    public function begin(): void {
        $req        = new Request('post', $this->baseUrl . 'transaction');
        $resp       = $this->sendRequest($req);
        $this->txId = $resp->getHeader($this->getHeaderName('transactionId'))[0];
    }

    /**
     * Rolls back the current transaction (started with `begin()`).
     * 
     * All data modifications must be performed within a transaction.
     * 
     * @return void
     * @see begin()
     * @see commit()
     */
    public function rollback(): void {
        if (!empty($this->txId)) {
            $headers    = [$this->getHeaderName('transactionId') => $this->txId];
            $req        = new Request('delete', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
            $this->txId = null;
        }
    }

    /**
     * Commits the current transaction (started with `begin()`).
     * 
     * All data modifications must be performed within a transaction.
     * 
     * @return void
     * @see begin()
     * @see rollback()
     */
    public function commit(): void {
        if (!empty($this->txId)) {
            $headers    = [$this->getHeaderName('transactionId') => $this->txId];
            $req        = new Request('put', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
            $this->txId = null;
        }
    }

    /**
     * Prolongs the current transaction (started with `begin()`).
     * 
     * Every repository has a transaction timeout. If there are no calls to the
     * repository 
     * 
     * @return void
     * @see begin()
     */
    public function prolong(): void {
        if (!empty($this->txId)) {
            $headers = [$this->getHeaderName('transactionId') => $this->txId];
            $req     = new Request('get', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
        }
    }

    /**
     * Checks if there is an active transaction.
     * 
     * @return bool
     * @see begin()
     * @see rollback()
     * @see commit()
     * @see prolong()
     */
    public function inTransaction(): bool {
        return !empty($this->txId);
    }

    /**
     * Parses search request response into an array of `RepoResource` objects.
     * 
     * @param ResponseInterface $resp PSR-7 search request response
     * @param SearchConfig $config search configuration object
     * @return Generator<RepoResource>
     */
    private function parseSearchResponse(ResponseInterface $resp,
                                         SearchConfig $config): Generator {
        $class = $config->class ?? self::$resourceClass;

        $graph = new Graph();
        $body  = $resp->getBody();
        if (!empty($body)) {
            $format = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
            $graph->parse($body, $format);

            $config->count = (int) ((string) $graph->resource($this->getBaseUrl())->getLiteral($this->getSchema()->searchCount));

            $resources = $graph->resourcesMatching($this->schema->searchMatch);
            foreach ($resources as $i) {
                $i->delete($this->schema->searchMatch);
                $obj = new $class($i->getUri(), $this);
                $obj->setGraph($i);
                yield $obj;
            }
        }
    }
}
