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

use rdfInterface\DatasetInterface;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\TermInterface;
use rdfInterface\QuadInterface;
use quickRdf\DataFactory as DF;
use termTemplates\QuadTemplate as QT;
use zozlak\RdfConstants as RDF;

/**
 * A common boilet plate code to be reused by all RepoResourceInterface
 * implementations.
 *
 * @author zozlak
 */
trait RepoResourceTrait {

    private DatasetNodeInterface $metadata;
    private bool $metaSynced;
    private RepoInterface $repoInt;

    /**
     * Returns the repository resource URL.
     */
    public function getUri(): TermInterface {
        return $this->metadata->getNode();
    }

    /**
     * Returns repository connection object associated with the given resource object.
     */
    public function getRepo(): RepoInterface {
        return $this->repoInt;
    }

    /**
     * Returns an array with all repository resource identifiers.
     * 
     * @return array<string>
     */
    public function getIds(): array {
        $idProp = $this->repoInt->getSchema()->id;
        $this->loadMetadata();
        return $this->metadata->listObjects(new QT(predicate: $idProp))->getValues();
    }

    /**
     * Returns all RDF types (classes) of a given repository resource.
     * 
     * @return array<string>
     */
    public function getClasses(): array {
        $this->loadMetadata();
        return $this->metadata->listObjects(new QT(predicate: DF::namedNode(RDF::RDF_TYPE)))->getValues();
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
     * @return DatasetNodeInterface
     * @see setMetadata()
     * @see setGraph()
     * @see getGraph()
     */
    public function getMetadata(): DatasetNodeInterface {
        $this->loadMetadata();
        return $this->metadata->copy();
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
     * @return DatasetNodeInterface
     * @see setGraph()
     * @see getMetadata()
     */
    public function getGraph(): DatasetNodeInterface {
        $this->loadMetadata();
        return $this->metadata;
    }

    /**
     * Replaces resource metadata with a given RDF resource graph. A deep copy
     * of the provided metadata is stored meaning future modifications of the
     * $metadata object don't affect the resource metadata.
     * 
     * New metadata is not automatically written back to the repository.
     * Use the `updateMetadata()` method to write them back.
     * 
     * @param DatasetNodeInterface $metadata
     * @see updateMetadata()
     * @see setGraph()
     */
    public function setMetadata(DatasetNodeInterface $metadata): void {
        $sbj              = $this->metadata->getNode();
        $metadata         = $metadata->withDataset($metadata->getDataset()->copy());
        $metadata->forEach(fn(QuadInterface $x) => $x->withSubject($sbj));
        $this->metadata   = $this->metadata->withDataset($metadata->getDataset());
        $this->metaSynced = false;
    }

    /**
     * Replaces resource metadata with a given RDF graph. A reference
     * to the provided metadata is stored meaning future modifications of the
     * $metadata object automatically affect the resource metadata.
     * 
     * New metadata are not automatically written back to the repository.
     * Use the updateMetadata() method to write them back.
     * 
     * @param DatasetInterface $metadata
     * @return void
     * @see updateMetadata()
     * @see setMetadata()
     */
    public function setGraph(DatasetInterface $metadata): void {
        $this->metadata   = $this->metadata->withDataset($metadata);
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
