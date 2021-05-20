<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
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

use PDO;
use Throwable;
use PDOException;
use RuntimeException;
use GuzzleHttp\Exception\RequestException;
use Psr\Log\AbstractLogger;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoDb;
use acdhOeaw\arche\lib\RepoResourceInterface;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\AmbiguousMatch;

/**
 * Returns repository resource object having a given id.
 * 
 * Automatically configures the repository connection (REST or direct database access)
 * based on the provided configutation.
 *
 * @author zozlak
 */
class RepoResourceResolver {

    private ?Config $config;
    private ?RepoInterface $repo;
    private ?AbstractLogger $log;

    /**
     * Sets up the resolver.
     * 
     * May work in three modes:
     * - zero-config mode (when `$config` is null). In this mode resolution is
     *   done by just trying to resolve the URL. It's the slowest mode but
     *   requires no configutation at all.
     *   This mode is always available as a fallback resolution method for the
     *   resolve() method and explicitely as the resolveUrl() method.
     * - REST API mode (when `$config` is provided and contains `$config->dbConnStr`)
     *   This mode is faster but works only against a single repository.
     * - direct database access mode (when `$config` is provided and contains
     *    `$config->dbConnStr`). This is definitely the fastest mode but works
     *    only against a single repository and requires direct database access.
     * 
     * If modes other than zero-config one are used, the `$config` parameter has to
     * contain a minimum repository description including `$config->rest->urlBase`, 
     * `$config->rest->pathBase`, `$config->rest->headers`, `$config->schema->id`,
     * `$config->schema->parent`, `$config->schema->label` and `$config->schema->searchMatch`.
     * 
     * In case of REST API mode credentials can be provided with 
     * `$config->auth->httpBasic->user` and `$config->auth->httpBasic->password`.
     * 
     * @param ?object $config
     * @param ?AbstractLogger $log
     */
    public function __construct(?object $config, ?AbstractLogger $log = null) {
        $this->log = $log;
        if ($config !== null) {
            $this->config = new Config($config);
            $schema       = new Schema($this->config->schema);
            $headers      = new Schema($this->config->rest->headers);
            $baseUrl      = $this->config->rest->urlBase . $this->config->rest->pathBase;
            $dbConnStr = $this->config->dbConnStr ?? ($this->config->dbConn->guest ?? '');
            if (!empty($dbConnStr)) {
                $pdo        = new PDO($dbConnStr);
                $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
                $this->repo = new RepoDb($baseUrl, $schema, $headers, $pdo);
            } else {
                $options    = (array) ($this->config->options ?? []);
                $this->repo = new Repo($baseUrl, $schema, $headers, $options);
            }
            if ($log !== null) {
                $this->repo->setQueryLog($log);
            }
        }
    }

    /**
     * Resolves a given resource URI according to the resolver settings.
     * 
     * @see __construct()
     * @param string $resId
     * @param bool $fallback Should fallback zero-config mode be used when
     *   a resource is not found? (valid only for non-zero-config modes - see the
     *   constructor descrription)
     * @return RepoResourceInterface
     * @throws RuntimeException
     */
    public function resolve(string $resId, bool $fallback = true): RepoResourceInterface {
        if ($this->repo !== null) {
            try {
                return $this->repo->getResourceById($resId);
            } catch (NotFound $e) {
                if (!$fallback) {
                    throw new RuntimeException('No repository resource found with a given URI', 400);
                }
            } catch (AmbiguousMatch $e) {
                throw new RuntimeException('Internal Server Error - many resources with the given URI', 500);
            } catch (RequestException $e) {
                throw new RuntimeException('Can not connect to the repository', 500);
            } catch (PDOException $e) {
                throw new RuntimeException('Can not connect to the repository database', 500);
            }
        }
        return self::resolveUrl($resId);
    }

    /**
     * Resolves a given URL in a zero-configuration mode.
     * 
     * @param string $url
     * @return RepoResourceInterface
     */
    public function resolveUrl(string $url): RepoResourceInterface {
        if ($this->log) {
            $this->log->debug("Resolving $url using a zero-config method");
        }
        $metaHeader = $this->config?->rest->headers->metadataReadMode;
        $realUrl    = null;
        $repo       = Repo::factoryFromUrl($url, [], $realUrl, $metaHeader);
        if ($this->log) {
            $this->log->info("$url resolved to $realUrl");
        }
        return new RepoResource($realUrl, $repo);
    }

    public function handleException(Throwable $e, ?AbstractLogger $log): void {
        if ($log !== null) {
            $log->error($e);
        }

        $code = $e->getCode();
        if ($code < 400) {
            $code = 500;
        }
        $msg    = $e->getMessage();
        $p      = strpos($msg, "\n");
        $reason = substr($msg, 0, (int) $p);
        $body   = substr($msg, $p + 1);
        if (empty($body)) {
            $body = $reason;
        }
        header("HTTP/1.1 $code $reason");
        echo "$msg\n";
    }
}
