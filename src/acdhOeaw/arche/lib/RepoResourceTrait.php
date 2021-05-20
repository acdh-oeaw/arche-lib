<?php

/*
 * The MIT License
 *
 * Copyright 2019 zozlak.
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

use EasyRdf\Resource;
use acdhOeaw\arche\lib\exception\RepoLibException;
use zozlak\RdfConstants as RDF;

/**
 * A common boilet plate code to be reused by all RepoResourceInterface
 * implementations.
 *
 * @author zozlak
 */
trait RepoResourceTrait {

    private ?Resource $metadata = null;
    private bool $metaSynced;
    private RepoInterface $repoInt;
    private string $url;

    /**
     * Returns the repository resource URL.
     * 
     * @return string
     */
    public function getUri(): string {
        return $this->url;
    }

    /**
     * Returns repository connection object associated with the given resource object.
     * 
     * @return RepoInterface
     */
    public function getRepo(): RepoInterface {
        return $this->repoInt;
    }

    /**
     * Returns an array with all repository resource identifiers.
     * 
     * @return string[]
     */
    public function getIds(): array {
        $idProp = $this->repoInt->getSchema()->id;
        $this->loadMetadata();
        $ids    = [];
        foreach ($this->metadata?->allResources($idProp) as $i) {
            $ids[] = (string) $i;
        }
        return $ids;
    }

    /**
     * Returns all RDF types (classes) of a given repository resource.
     * 
     * @return array<string>
     */
    public function getClasses(): array {
        $this->loadMetadata();
        $ret = [];
        foreach ($this->metadata?->allResources(RDF::RDF_TYPE) as $i) {
            $ret[] = $i->getUri();
        }
        return $ret;
    }

    /**
     * Returns resource metadata.
     * 
     * Fetches them from the repository with the `loadMetadata()` if they were 
     * not fetched already.
     * 
     * A deep copy of metadata is returned meaning adjusting the returned object
     * does not automatically affect the resource metadata.
     * Use the setMetadata() method to write back the changes you made.
     * 
     * @return Resource
     * @see setMetadata()
     * @see setGraph()
     * @see getGraph()
     */
    public function getMetadata(): Resource {
        $this->loadMetadata();
        return $this->metadata?->copy() ?? throw new RepoLibException('Metadata not loaded');
    }

    /**
     * Returns resource metadata.
     * 
     * Fetches them from the repository with the `loadMetadata()` if they were 
     * not fetched already.
     * 
     * A reference to the metadata is returned meaning adjusting the returned object
     * automatically affects the resource metadata.
     * 
     * @return Resource
     * @see setGraph()
     * @see getMetadata()
     */
    public function getGraph(): Resource {
        $this->loadMetadata();
        return $this->metadata ?? throw new RepoLibException('Metadata not loaded');
    }

    /**
     * Replaces resource metadata with a given RDF resource graph. A deep copy
     * of the provided metadata is stored meaning future modifications of the
     * $metadata object don't affect the resource metadata.
     * 
     * New metadata are not automatically written back to the repository.
     * Use the `updateMetadata()` method to write them back.
     * 
     * @param Resource $metadata
     * @see updateMetadata()
     * @see setGraph()
     */
    public function setMetadata(Resource $metadata): void {
        $this->metadata   = $metadata->copy([], '/^$/', $this->getUri());
        $this->metaSynced = false;
    }

    /**
     * Replaces resource metadata with a given RDF resource graph. A reference
     * to the provided metadata is stored meaning future modifications of the
     * $metadata object automatically affect the resource metadata.
     * 
     * New metadata are not automatically written back to the repository.
     * Use the updateMetadata() method to write them back.
     * 
     * @param Resource $resource
     * @return void
     * @see updateMetadata()
     * @see setMetadata()
     */
    public function setGraph(Resource $resource): void {
        $this->metadata   = $resource;
        $this->metaSynced = false;
    }

    /**
     * Naivly checks if the resource is of a given class.
     * 
     * Naivly means that a given rdfs:type triple must exist in the resource
     * metadata.
     * 
     * @param string $class
     * @return bool
     */
    public function isA(string $class): bool {
        return in_array($class, $this->getClasses());
    }

    abstract public function loadMetadata();
}
