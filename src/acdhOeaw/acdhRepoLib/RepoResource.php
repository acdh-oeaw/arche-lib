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
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

/**
 * Description of RepoResource
 *
 * @author zozlak
 */
class RepoResource {

    const META_RESOURCE  = 'resource';
    const META_NEIGHBORS = 'neighbors';
    const META_RELATIVES = 'relatives';

    /**
     *
     * @var acdhOeaw\acdhRepoLib\Repo
     */
    private $repo;

    /**
     *
     * @var string
     */
    private $uri;

    /**
     *
     * @var EasyRdf\Resource
     */
    private $metadata;

    /**
     *
     * @var bool
     */
    private $metaSynced;

    public function __construct(string $uri, Repo $repo) {
        $this->uri  = $uri;
        $this->repo = $repo;
    }

    public function getRepo(): Repo {
        return $this->repo;
    }

    public function getUri(): string {
        return $this->uri;
    }

    public function getIds(): array {
        $idProp = $this->repo->getSchema()->id;
        $this->loadMetadata();
        $ids    = array();
        foreach ($this->metadata->allResources($idProp) as $i) {
            $ids[] = $i->getUri();
        }
        return $ids;
    }

    /**
     * Returns all RDF types (classes) of a given resource.
     * @return array
     */
    public function getClasses(): array {
        $this->loadMetadata();
        $ret = array();
        foreach ($this->metadata->allResources('http://www.w3.org/1999/02/22-rdf-syntax-ns#type') as $i) {
            $ret[] = $i->getUri();
        }
        return $ret;
    }

    /**
     * Returns resource's binary content.
     * @return Response PSR-7 response containing resource's binary content
     */
    public function getContent(): Response {
        $request = new Request('get', $this->uri);
        return $this->repo->sendRequest($request);
    }

    public function updateContent(BinaryPayload $content): void {
        $request = new Request('put', $this->uri);
        $request = $content->attachTo($request);
        $this->repo->sendRequest($request);
        $this->loadMetadata(true);
    }

    /**
     * Returns resource metadata.
     * 
     * Fetches them from the repository if they were not fetched already.
     * 
     * A deep copy of metadata is returned meaning adjusting the returned object
     * does not automatically affect the resource metadata.
     * Use the setMetadata() method to write back the changes you made.
     * 
     * @param bool $force enforce fetch from the repository 
     * @return \EasyRdf\Resource
     * @see setMetadata()
     * @see setGraph()
     * @see getGraph()
     */
    public function getMetadata(bool $force = false): Resource {
        $this->loadMetadata($force);
        return $this->metadata->copy();
    }

    /**
     * Returns resource metadata.
     * 
     * Fetches them from the repository if they were not fetched already.
     * 
     * A reference to the metadata is returned meaning adjusting the returned object
     * automatically affects the resource metadata.
     * 
     * @param bool $force enforce fetch from the repository 
     * @return \EasyRdf\Resource
     * @see setGraph()
     * @see getMetadata()
     */
    public function getGraph(bool $force = false): Resource {
        $this->loadMetadata($force);
        return $this->metadata;
    }

    /**
     * Naivly checks if the resource is of a given class.
     * 
     * Naivly means that a given rdfs:type triple must exist in the resource
     * metadata.
     * 
     * @param type $class
     * @return bool
     */
    public function isA(string $class): bool {
        return in_array($class, $this->getClasses());
    }

    /**
     * Checks if the resource has any binary content
     * @return bool
     */
    public function hasBinaryContent(): bool {
        $this->getMetadata();
        return (int) ((string) $this->metadata->getLiteral($this->repo->getSchema()->binarySize)) > 0;
    }

    /**
     * Replaces resource metadata with a given RDF resource graph. A deep copy
     * of the provided metadata is stored meaning future modifications of the
     * $metadata object don't affect the resource metadata.
     * 
     * New metadata are not automatically written back to the repository.
     * Use the updateMetadata() method to write them back.
     * 
     * @param EasyRdf\Resource $metadata
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
     * @param EasyRdf\Resource $resource
     * @return void
     * @see updateMetadata()
     * @see setMetadata()
     */
    public function setGraph(Resource $resource): void {
        $this->metadata   = $resource;
        $this->metaSynced = false;
    }

    public function updateMetadata(): void {
        if (!$this->metaSynced) {
            $headers          = [
                'Content-Type' => 'application/n-triples',
                'Accept'       => 'application/n-triples',
            ];
            $body             = $this->metadata->getGraph()->serialise('application/n-triples');
            $req              = new Request('patch', $this->uri . '/metadata', $headers, $body);
            $resp             = $this->repo->sendRequest($req);
            $this->parseMetadata($resp);
            $this->metaSynced = true;
        }
    }

    public function delete(bool $tombstone = false, bool $references = false): void {
        $req = new Request('delete', $this->getUri());
        $this->repo->sendRequest($req);

        if ($tombstone) {
            $req = new Request('delete', $this->getUri() . '/tombstone');
            $this->repo->sendRequest($req);
        }

        if ($references) {
            $query  = "SELECT id FROM relations WHERE target_id = ?";
            $refRes = $this->repo->getResourcesBySqlQuery($query, [$this->getId()]);
            foreach ($refRes as $res) {
                /* @var $res \acdhOeaw\acdhRepoLib\RepoResource */
                $meta = $res->getMetadata();
                foreach ($meta->propertyUris() as $p) {
                    $meta->deleteResource($p, $this->getUri());
                    if (null === $meta->getResource($p)) {
                        $meta->addResource($this->repo->getSchema()->delete, $p);
                    }
                }
                $res->setMetadata($meta);
                $res->updateMetadata();
            }
        }
    }

    public function deleteRecursively(string $property, bool $tombstone = false,
                                      bool $references = false): void {
        $query  = "SELECT id FROM relations WHERE property = ? AND target_id = ?";
        $refRes = $this->repo->getResourcesBySqlQuery($query, [$property, $this->getId()]);
        foreach ($refRes as $res) {
            /* @var $res \acdhOeaw\acdhRepoLib\RepoResource */
            $res->deleteRecursively($property, $tombstone, $references);
        }
        $this->delete($tombstone, $references);
    }

    /**
     * Loads current metadata from the Fedora.
     * 
     * @param bool $force enforce fetch from Fedora 
     *   (when you want to make sure metadata are in line with ones in the Fedora 
     *   or e.g. reset them back to their current state in Fedora)
     */
    protected function loadMetadata(bool $force = false): void {
        if (!$this->metadata || $force) {
            $headers = [
                'Accept' => 'application/n-triples',
            ];
            $req     = new Request('get', $this->uri . '/metadata', $headers);
            $resp    = $this->repo->sendRequest($req);
            $this->parseMetadata($resp);
        }
    }

    private function parseMetadata(Response $resp): void {
        $format           = explode(';', $resp->getHeader('Content-Type')[0] ?? '')[0];
        $graph            = new Graph();
        $graph->parse($resp->getBody(), $format);
        $this->metadata   = $graph->resource($this->uri);
        $this->metaSynced = true;
    }

    protected function getId(): int {
        return (int) substr($this->getUri(), strlen($this->repo->getBaseUrl()));
    }

}
