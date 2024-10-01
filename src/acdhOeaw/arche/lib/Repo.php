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
use RuntimeException;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\Exception\TransferException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Promise\RejectedPromise;
use Psr\Http\Message\ResponseInterface;
use rdfInterface\DatasetInterface;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\QuadInterface;
use rdfInterface\TermInterface;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\NQuadsSerializer;
use quickRdfIo\Util as RdfIoUtil;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;
use acdhOeaw\arche\lib\exception\RepoLibException;
use acdhOeaw\arche\lib\promise\RepoResourceGeneratorPromise;
use acdhOeaw\arche\lib\promise\GraphPromise;
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

    const REJECT_SKIP            = 1;
    const REJECT_FAIL            = 2;
    const REJECT_INCLUDE         = 3;
    const ARCHE_CORE_MIN_VERSION = 3.2;
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
        $options            = [];
        $options['headers'] = (array) ($config->auth->httpHeader ?? []);
        if (!empty($config->auth->httpBasic->user ?? '')) {
            $options['auth'] = [$config->auth->httpBasic->user, $config->auth->httpBasic->password ?? ''];
        }
        if (($config->rest->verifyCert ?? true) === false) {
            $options['verify'] = false;
        }

        return new Repo($baseUrl, $options);
    }

    /**
     * Interactively creates a repository instance asking user for all required data.
     * 
     * @param string|null $cfgLocation optional path to a config file.
     *   The config file may provide a predefined list of repositories and/or 
     * ARCHE login formatted as follows:
     *   ```
     *   repositories:
     *   - urlBase: http://arche.acdh.oeaw.ac.at
     *     pathBase: /api/
     *   - urlBase: https://arche-curation.acdh-dev.oeaw.ac.at
     *     pathBase: /
     *   auth:
     *     httpBasic:
     *       user: myLogin
     *   ```
     * @param string|null $login ARCHE login. If null, user will be asked to provide it interactively.
     * @param string|null $pswd ARCHE password. If null, user will be asked to provide it interactively.
     * @return self
     */
    static public function factoryInteractive(?string $cfgLocation = null,
                                              ?string $login = null,
                                              ?string $pswd = null): self {
        $cfg = null;
        if ($cfgLocation !== null) {
            if (!file_exists($cfgLocation) || !is_file($cfgLocation)) {
                echo "No config.yaml found.\n";
            } else {
                echo "Configuration found at $cfgLocation\n";
                $cfg = Config::fromYaml($cfgLocation);
            }
        }

        if (isset($cfg->repositories)) {
            echo "\nWhat's the repository you want to ingest to? (type a number)\n";
            foreach ($cfg->repositories as $k => $v) {
                echo ($k + 1) . "\t" . $v->urlBase . $v->pathBase . "\n";
            }
            $line    = ((int) trim((string) fgets(\STDIN))) - 1;
            $baseUrl = ($cfg->repositories[$line]->urlBase ?? '') . ($cfg->repositories[$line]->pathBase ?? '');
        } else {
            echo "\nWhat's the base URL of the repository you want to ingest to?\n";
            $baseUrl = trim((string) fgets(\STDIN));
        }
        echo "\nIs repository URL $baseUrl correct? (type 'yes' to continue)\n";
        $line = trim((string) fgets(\STDIN));
        if ($line !== 'yes') {
            echo "Wrong repository URL\n";
            exit(1);
        }

        $user = $login ?? ($cfg->auth->httpBasic->user ?? '');
        if (empty($user)) {
            echo "\nWhat's your login?\n";
            $user = trim((string) fgets(\STDIN));
        }

        if (empty($pswd)) {
            echo "\nWhat's your password?\n";
            system('stty -echo');
            $pswd = trim((string) fgets(\STDIN));
            system('stty echo');
        }

        $options = ['auth' => [$user, $pswd]];
        $repo    = self::factoryFromUrl($baseUrl, $options);
        return $repo;
    }

    /**
     * Creates a Repo instance from any URL pointing (also trough redirects) 
     * to a valid REST API endpoint.
     * 
     * It can be slow (especially when redirects are involved) but requires 
     * no config.
     * 
     * @param string $url
     * @param array<mixed> $guzzleOptions
     * @param string|null $realUrl if provided, the final resource URL will be stored
     *   in this variable.
     * @param string|null $metaReadModeHeader header used by the repository to denote
     *   the metadata read mode. Providing this parameter will speed up the
     *   initialization if the $url points to a repository resource.
     * @return self
     */
    static public function factoryFromUrl(string $url,
                                          array $guzzleOptions = [],
                                          ?string &$realUrl = null,
                                          ?string $metaReadModeHeader = null): self {
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
        $realUrl   = (string) preg_replace('`/metadata/?$`', '', $realUrl);
        $baseUrl   = (string) preg_replace('`/?(|describe|user|user/[^/]+|metadata|transaction|[0-9]+|[0-9]+/tombstone|merge/[0-9]+/[0-9]+|search)/?$`', '', $realUrl);

        return new Repo($baseUrl, $guzzleOptions);
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
    private string $baseUrl;
    private string $baseUri;

    /**
     * Creates an repository connection object.
     * 
     * @param string $baseUrl repository REST API base URL
     * @param array<mixed> $guzzleOptions Guzzle HTTP client connection options to be used 
     *   by all requests to the repository REST API (e.g. credentials)
     */
    public function __construct(string $baseUrl, array $guzzleOptions = []) {
        $this->client = new Client($guzzleOptions);

        $this->baseUrl = $baseUrl;
        if (substr($this->baseUrl, -1) !== '/') {
            $this->baseUrl .= '/';
        }

        $headers  = ['Accept' => 'application/json'];
        $response = $this->client->send(new Request('get', $this->baseUrl . "describe", $headers));
        if ($response->getStatusCode() !== 200) {
            throw new NotFound("$baseUrl doesn't resolve to an ARCHE repository", 404);
        }
        $config  = new Config((object) json_decode((string) $response->getBody()));
        $version = (float) ($config->version ?? 0.1);
        if ($version > 0 && $version < self::ARCHE_CORE_MIN_VERSION) {
            throw new RepoLibException("This version of arche-lib requires ARCHE version " . self::ARCHE_CORE_MIN_VERSION . " while the repository version is $version");
        }
        $this->schema  = new Schema($config->schema);
        $this->headers = $config->rest->headers;
        $this->baseUri = $config->rest->urlBase . $config->rest->pathBase;
    }

    /**
     * Returns the repository REST API base URL.
     * 
     * @return string
     */
    public function getBaseUrl(): string {
        return $this->baseUri;
    }

    /**
     * Creates a repository resource.
     * 
     * @param DatasetNodeInterface $metadata resource metadata
     * @param BinaryPayload|null $payload resource binary payload (can be null)
     * @param string|null $class an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @param string $readMode scope of the metadata returned by the repository
     *   - see the META_* constants defined by the RepoResourceInterface 
     * @param string|null $parentProperty RDF property to be used by the metadata
     *   read mode denoted by the $readMode parameter
     * @return RepoResource
     */
    public function createResource(DatasetNodeInterface $metadata,
                                   ?BinaryPayload $payload = null,
                                   ?string $class = null,
                                   string $readMode = RepoResourceInterface::META_RESOURCE,
                                   ?string $parentProperty = null): RepoResource {
        return $this->createResourceAsync($metadata, $payload, $class, $readMode, $parentProperty)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of createResource()
     * 
     * @param DatasetNodeInterface $metadata
     * @param BinaryPayload|null $payload
     * @param string|null $class
     * @param string $readMode
     * @param string|null $parentProperty
     * @return RepoResourcePromise
     * @see createResource()
     */
    public function createResourceAsync(DatasetNodeInterface $metadata,
                                        ?BinaryPayload $payload = null,
                                        ?string $class = null,
                                        string $readMode = RepoResourceInterface::META_RESOURCE,
                                        ?string $parentProperty = null): RepoResourcePromise {
        $sbj         = DF::namedNode($this->baseUri);
        $metadata    = $metadata->map(fn(QuadInterface $x) => $x->withSubject($sbj))->withNode($sbj);
        $serializer  = new NQuadsSerializer();
        $body        = $serializer->serialize($metadata);
        $headers     = ['Content-Type' => 'application/n-triples'];
        $req         = new Request('post', $this->baseUrl . 'metadata', $headers, $body);
        $readModeTmp = $payload === null ? $readMode : RepoResourceInterface::META_NONE;
        $req         = $this->withReadHeaders($req, $readModeTmp, $parentProperty);
        $promise     = $this->sendRequestAsync($req);
        $promise     = $promise->then(function (ResponseInterface $resp) use ($payload,
                                                                              $class,
                                                                              $readMode,
                                                                              $parentProperty): RepoResource | RepoResourcePromise {
            $class = $class ?? self::$resourceClass;
            $res   = $class::factory($this, $resp);

            if ($payload === null) {
                return $res;
            }
            $promise = $res->updateContentAsync($payload, $readMode, $parentProperty);
            $promise = $promise->then(fn() => $res);
            return new RepoResourcePromise($promise);
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
        return $this->sendRequestAsync($request)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of sendRequest()
     * 
     * @param Request $request
     * @return ResponsePromise
     * @see sendRequest()
     */
    public function sendRequestAsync(Request $request): ResponsePromise {
        $request = $request->withUri(new Uri(str_replace($this->baseUri, $this->baseUrl, (string) $request->getUri())));
        if (!empty($this->txId)) {
            $request = $request->withHeader($this->getHeaderName('transactionId'), $this->txId);
        }
        $promise = $this->client->sendAsync($request)->otherwise(
            function (TransferException $e) {
                switch ($e->getCode()) {
                    case 410:
                        return new RejectedPromise(new Deleted());
                    case 409:
                        if ($e instanceof RequestException) {
                            return new RejectedPromise(new Conflict((string) $e->getResponse()?->getBody()));
                        } else {
                            return new RejectedPromise($e);
                        }
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
     *   Repo::REJECT_SKIP (skip them silently), Repo::REJECT_FAIL (throw an error)
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
        $param     = [
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
        ];
        $queue = new \GuzzleHttp\Promise\EachPromise($promiseIterator($iter, $func), $param);
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
     * @param string $class|null an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @return RepoResource
     * @throws NotFound
     * @throws AmbiguousMatch
     */
    public function getResourceByIds(array $ids, ?string $class = null): RepoResource {
        return $this->getResourceByIdsAsync($ids, $class)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of getResourceByIds()
     * 
     * @param string $id
     * @param string|null $class
     * @return RepoResourcePromise
     * @see getResourceByIds()
     */
    public function getResourceByIdAsync(string $id, ?string $class = null): RepoResourcePromise {
        return $this->getResourceByIdsAsync([$id], $class);
    }

    /**
     * Asynchronous version of getResourceByIds()
     * 
     * @param array<string> $ids
     * @param string|null $class
     * @return RepoResourcePromise
     * @see getResourceByIds()
     */
    public function getResourceByIdsAsync(array $ids, ?string $class = null): RepoResourcePromise {
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
        $promise      = $this->sendRequestAsync($req);
        $promise      = $promise->then(function (ResponseInterface $resp) use ($class) {
            $graph   = new Dataset();
            $graph->add(RdfIoUtil::parse($resp, new DF()));
            $matches = $graph->listSubjects(new QT(predicate: $this->schema->searchMatch))->getValues();
            switch (count($matches)) {
                case 0:
                    return new RejectedPromise(new NotFound());
                case 1;
                    $class = $class ?? self::$resourceClass;
                    $res   = new $class($matches[0], $this);
                    $graph->delete(new PT($this->schema->searchMatch));
                    $graph->delete(new PT($this->schema->searchOrder));
                    $graph->delete(new PT($this->schema->searchOrderValue));
                    $res->setGraph($graph);
                    return $res;
                default:
                    return new RejectedPromise(new AmbiguousMatch("Many resources match the search: " . implode(', ', $matches)));
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
        return $this->getResourcesBySqlQueryAsync($query, $parameters, $config)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of getResourcesBySqlQuery()
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return RepoResourceGeneratorPromise
     * @see getResourcesBySqlQuery()
     */
    public function getResourcesBySqlQueryAsync(string $query,
                                                array $parameters,
                                                SearchConfig $config): RepoResourceGeneratorPromise {
        $promise = $this->getGraphBySqlQueryAsync($query, $parameters, $config);
        $promise = $promise->then(function (Dataset $graph) use ($config): Generator {
            yield from $this->extractResourcesFromGraph($graph, $config);
        });
        return new RepoResourceGeneratorPromise($promise);
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
        return $this->getResourcesBySearchTermsAsync($searchTerms, $config)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of getResourcesBySearchTerms()
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return RepoResourceGeneratorPromise
     * @see getResourcesBySearchTerms()
     */
    public function getResourcesBySearchTermsAsync(array $searchTerms,
                                                   SearchConfig $config): RepoResourceGeneratorPromise {
        $promise = $this->getGraphBySearchTermsAsync($searchTerms, $config);
        $promise = $promise->then(function (Dataset $graph) use ($config): Generator {
            yield from $this->extractResourcesFromGraph($graph, $config);
        });
        return new RepoResourceGeneratorPromise($promise);
    }

    /**
     * Asynchronous version of 
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return DatasetInterface
     */
    public function getGraphBySqlQuery(string $query, array $parameters,
                                       SearchConfig $config): DatasetInterface {
        return $this->getGraphBySqlQueryAsync($query, $parameters, $config)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of getGraphBySqlQuery()
     * 
     * @param string $query
     * @param array<mixed> $parameters
     * @param SearchConfig $config
     * @return GraphPromise
     * @see getGraphBySqlQuery()
     */
    public function getGraphBySqlQueryAsync(string $query, array $parameters,
                                            SearchConfig $config): GraphPromise {
        $headers    = [
            'Accept'       => 'application/n-triples',
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $headers    = array_merge($headers, $config->getHeaders($this));
        $parameters = array_map(fn($x) => $x instanceof TermInterface ? $x->getValue() : $x, $parameters);
        $body       = array_merge(
            ['sql' => $query, 'sqlParam' => $parameters],
            $config->toArray()
        );
        $body       = http_build_query($body);
        $req        = new Request('post', $this->baseUrl . 'search', $headers, $body);
        $promise    = $this->sendRequestAsync($req);
        $promise    = $promise->then(function (ResponseInterface $resp) use ($config): Dataset {
            return $this->parseSearchResponse($resp, $config);
        });
        return new GraphPromise($promise);
    }

    /**
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return Dataset
     */
    public function getGraphBySearchTerms(array $searchTerms,
                                          SearchConfig $config): Dataset {
        return $this->getGraphBySearchTermsAsync($searchTerms, $config)->wait(true) ?? throw new RuntimeException('Promise returned null');
    }

    /**
     * Asynchronous version of getGraphBySearchTerms()
     * 
     * @param array<SearchTerm> $searchTerms
     * @param SearchConfig $config
     * @return GraphPromise
     * @see getGraphBySearchTerms()
     */
    public function getGraphBySearchTermsAsync(array $searchTerms,
                                               SearchConfig $config): GraphPromise {
        $headers = [
            'Accept'       => 'application/n-triples',
            'Content-Type' => 'application/x-www-form-urlencoded',
        ];
        $headers = array_merge($headers, $config->getHeaders($this));
        $body    = [];
        $n       = 0;
        foreach ($searchTerms as $i) {
            $body[] = $i->getFormData($n++);
        }
        $body = implode('&', $body);
        $body .= (!empty($body) ? '&' : '') . $config->toQuery();
        $req  = new Request('post', $this->baseUrl . 'search', $headers, $body);

        $promise = $this->sendRequestAsync($req);
        $promise = $promise->then(function (ResponseInterface $resp) use ($config): Dataset {
            return $this->parseSearchResponse($resp, $config);
        });
        return new GraphPromise($promise);
    }

    /**
     * Begins a transaction.
     * 
     * All data modifications must be performed within a transaction.
     * 
     * @return string transaction identifier
     * @see rollback()
     * @see commit()
     */
    public function begin(): string {
        $this->txId = null;
        $req        = new Request('post', $this->baseUrl . 'transaction');
        $resp       = $this->sendRequest($req);
        $this->txId = $resp->getHeader($this->getHeaderName('transactionId'))[0];
        return $this->txId;
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
            $this->txId = null;
            $req        = new Request('delete', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
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
            $this->txId = null;
            $req        = new Request('put', $this->baseUrl . 'transaction', $headers);
            $this->sendRequest($req);
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
     * Parses search request response into the EasyRdf Graph.
     * 
     * @param ResponseInterface $resp PSR-7 search request response
     * @param SearchConfig $config search configuration object
     * @return Dataset
     */
    private function parseSearchResponse(ResponseInterface $resp,
                                         SearchConfig $config): Dataset {
        $graph         = new Dataset();
        $graph->add(RdfIoUtil::parse($resp, new DF()));
        $config->count = (int) ($graph->getObjectValue(new QT(DF::namedNode($this->getBaseUrl()), $this->getSchema()->searchCount)) ?? 0);
        return $graph;
    }

    private function withReadHeaders(Request $request, string $mode,
                                     ?string $parentProperty): Request {
        return $request->
                withHeader('Accept', 'application/n-triples')->
                withHeader($this->getHeaderName('metadataReadMode'), $mode)->
                withHeader($this->getHeaderName('metadataParentProperty'), $parentProperty ?? $this->schema->parent->getValue());
    }
}
