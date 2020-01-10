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

use Psr\Log\AbstractLogger;

/**
 * A common (mostly boiler plate) code for classes implementing the RepoInterface.
 *
 * @author zozlak
 */
trait RepoTrait {
    
    /**
     * A class used to instantiate objects representing repository resources.
     * 
     * To be used by external libraries extending the RepoResource class funcionality provided by this library.
     * 
     * @var string
     */
    static public $resourceClass = '\acdhOeaw\acdhRepoLib\RepoResource';

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
     * 
     * @var \Psr\Log\AbstractLogger
     */
    private $queryLog;

    /**
     * Returns the repository REST API base URL.
     * 
     * @return string
     */
    public function getBaseUrl(): string {
        return $this->baseUrl;
    }

    /**
     * Returns the `Schema` object defining repository entities to RDF property mappings.
     * 
     * @return \acdhOeaw\acdhRepoLib\Schema
     */
    public function getSchema(): Schema {
        return $this->schema;
    }

    /**
     * Returns an HTTP header name to be used to pass a given information in the repository request.
     * 
     * @param string $purpose
     * @return string|null
     */
    public function getHeaderName(string $purpose): ?string {
        return $this->headers->$purpose ?? null;
    }

    /**
     * Tries to find a repository resource with a given id.
     * 
     * Throws an error on failure.
     * 
     * @param string $id
     * @param string $class an optional class of the resulting object representing the resource
     *   (to be used by extension libraries)
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     */
    public function getResourceById(string $id, string $class = null): RepoResourceInterface {
        return $this->getResourceByIds([$id], $class);
    }

    /**
     * Sets a search queries logger
     * 
     * @param AbstractLogger $log
     * @return void
     */
    public function setQueryLog(AbstractLogger $log): void {
        $this->queryLog = $log;
    }

}