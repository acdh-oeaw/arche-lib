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

use rdfInterface\DatasetInterface;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\TermInterface;

/**
 *
 * @author zozlak
 */
interface RepoResourceInterface {

    /**
     * Provides no metadata.
     */
    const META_NONE = 'none';

    /**
     * Provide only given resource's metadata
     */
    const META_RESOURCE = 'resource';

    /**
     * Include metadata of all resources a given one points to and all resources
     * which point to it. If parentProperty is specified, only resources
     * pointing to a given one with a specified RDF predicate are included.
     */
    const META_NEIGHBORS = 'neighbors';

    /**
     * Include metadata of all resources which can be reached from a given one
     * by following (in both directions) an RDF predicate specified by the parentProperty
     * as well as all metadata of all resources a given resource points to.
     */
    const META_RELATIVES = 'relatives';

    /**
     * Include metadata of all resources which can be reached from a given one
     * by following (in both directions) an RDF predicate specified by the parentProperty
     */
    const META_RELATIVES_ONLY = 'relativesOnly';

    /**
     * Include metadata of all resources which can be reached from a given one
     * by following (in both directions) an RDF predicate specified by the parentProperty
     * as well as all metadata of all resources a given resource points to and
     * all resources pointing to a given one.
     */
    const META_RELATIVES_REVERSE = 'relativesReverse';

    /**
     * Like `relatives` but follows the parentProperty predicate only from subject
     * to object.
     */
    const META_PARENTS = 'parents';

    /**
     * Like `relativesOnly` but follows the parentProperty predicate only from 
     * subject to object.
     */
    const META_PARENTS_ONLY = 'parentsOnly';

    /**
     * Like `relativesReverse` but follows the parentProperty predicate only 
     * from subject to object.
     */
    const META_PARENTS_REVERSE = 'parentsReverse';

    /**
     * Provide only a `resourceUrl titleProperty title` triple for a requested
     * resource/resources matching the search.
     */
    const META_IDS = 'ids';

    /**
     * Creates an object representing a repository resource.
     * 
     * @param string $url URL of the resource
     * @param RepoInterface $repo repository connection object
     */
    public function __construct(string $url, RepoInterface $repo);

    /**
     * Returns the repository resource URL.
     */
    public function getUri(): TermInterface;

    /**
     * Returns repository connection object associated with the given resource object.
     */
    public function getRepo(): RepoInterface;

    /**
     * Returns an array with all repository resource identifiers.
     * 
     * @return array<string>
     */
    public function getIds(): array;

    /**
     * Loads current metadata from the repository.
     * 
     * @param bool $force enforce fetch from the repository 
     *   (when you want to make sure metadata are in line with ones in the repository 
     *   or e.g. reset them back to their current state in the repository)
     * @param string $mode scope of the metadata returned by the repository - 
     *   one of `RepoResourceInterface::META_*` constants.
     * @param string $parentProperty RDF property name used to find related 
     *   resources in some modes
     * @param array<string> $resourceProperties list of RDF properties to be includes
     *   for a resource (if the list is empty, all exsiting RDF properties are included)
     * @param array<string> $relativesProperties list of RDF properties to be includes
     *   for resources being relatives (if the list is empty, all exsiting RDF 
     *   properties are included)
     * @see RepoResourceInterface::META_RESOURCE
     */
    public function loadMetadata(bool $force = false,
                                 string $mode = self::META_RESOURCE,
                                 ?string $parentProperty = null,
                                 array $resourceProperties = [],
                                 array $relativesProperties = []): void;

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
     * @see getMetadata()
     */
    public function getGraph(): DatasetNodeInterface;

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
     * @see getGraph()
     */
    public function getMetadata(): DatasetNodeInterface;

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
    public function setGraph(DatasetInterface $metadata): void;

    /**
     * Replaces resource metadata with a given RDF resource graph. A deep copy
     * of the provided metadata is stored meaning future modifications of the
     * $metadata object don't affect the resource metadata.
     * 
     * New metadata are not automatically written back to the repository.
     * Use the `updateMetadata()` method to write them back.
     * 
     * @param DatasetNodeInterface $metadata
     * @see updateMetadata()
     * @see setGraph()
     */
    public function setMetadata(DatasetNodeInterface $metadata): void;

    /**
     * Naivly checks if the resource is of a given class.
     * 
     * Naivly means that a given rdfs:type triple must exist in the resource
     * metadata.
     * 
     * @param string $class
     * @return bool
     */
    public function isA(string $class): bool;

    /**
     * Returns all RDF types (classes) of a given repository resource.
     * 
     * @return array<string>
     */
    public function getClasses(): array;
}
