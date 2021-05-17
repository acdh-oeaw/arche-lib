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

/**
 * Stores the repository search configuration, e.g. full text search options and pagination options.
 *
 * @author zozlak
 */
class SearchConfig {

    const FTS_BINARY = 'BINARY';

    /**
     * Creates an instance of the SearchConfig class form the POST data.
     * 
     * @return SearchConfig
     */
    static public function factory(): SearchConfig {
        $sc = new SearchConfig();
        foreach ((array) $sc as $k => $v) {
            if (isset($_POST[$k])) {
                $sc->$k = $_POST[$k];
            } elseif (isset($_POST[$k . '[]'])) {
                $sc->$k = $_POST[$k . '[]'];
            }
        }

        return $sc;
    }

    /**
     * Controls amount of metadata included in the search results.
     * 
     * Value should be one of `RepoResourceInterface::META_*` constants.
     * 
     * @see \acdhOeaw\arche\lib\RepoResourceInterface::META_RESOURCE
     */
    public string $metadataMode;

    /**
     * RDF predicate used by some of metadataModes.
     */
    public ?string $metadataParentProperty;

    /**
     * Maximum number of returned resources (only resources matched by the search
     * are counted - see `$metadataMode`).
     */
    public int $limit;

    /**
     * Offset of the first returned result.
     * 
     * Remember your search results must be ordered if you want get stable results.
     */
    public int $offset;

    /**
     * Total number of resources matching the search (despite limit/offset)
     * 
     * Set by RepoInterface::getGraphBy*() and RepoInterface::getResourceBy*() 
     * methods.
     */
    public int $count;

    /**
     * List of metadata properties to order results by.
     * 
     * Only literal values are used for ordering.
     * 
     * @var array<string>
     */
    public array $orderBy = [];

    /**
     * If specified, only property values with a given language are taken into
     * account for ordering search matches.
     */
    public string $orderByLang;

    /**
     * A full text search query used for search results highlighting.
     * 
     * See https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-PARSING-QUERIES
     * and the websearch_to_tsquery() function documentation.
     * 
     * Remember this query is applied only to the search results and is not used to
     * perform an actual search (yes, technically you can search by one term
     * and highlight results using the other).
     */
    public string $ftsQuery;

    /**
     * Data to be used for full text search results highlighting.
     * 
     * - `null` if both resource metadata and binary content should be used;
     * - an RDF property if a given metadata property should be used
     * - `SearchConfig::FTS_BINARY` if the resource binary content should be used
     */
    public string $ftsProperty;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public string $ftsStartSel;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public string $ftsStopSel;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public int $ftsMaxWords;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public int $ftsMinWords;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public int $ftsShortWord;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public bool $ftsHighlightAll;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public int $ftsMaxFragments;

    /**
     * Full text search highlighting options see - https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-HEADLINE
     */
    public string $ftsFragmentDelimiter;

    /**
     * An optional class of the for the objects returned as the search results
     *   (to be used by extension libraries).
     */
    public string $class;

    /**
     * 
     * @return array<mixed>
     */
    public function toArray(): array {
        $a = [];
        foreach ((array) $this as $k => $v) {
            if (!in_array($k, ['class', 'metadataMode', 'metadataParentProperty']) && !empty($v)) {
                $a[$k] = $v;
            }
        }
        return $a;
    }

    /**
     * Returns HTTP request headers setting metadata read mode and metadata parent property
     * according to the search config settings.
     * 
     * @param \acdhOeaw\arche\lib\Repo $repo
     * @return array<string>
     */
    public function getHeaders(Repo $repo): array {
        $h = [];
        if (!empty($this->metadataMode)) {
            $h[$repo->getHeaderName('metadataReadMode')] = $this->metadataMode;
        }
        if (!empty($this->metadataParentProperty)) {
            $h[$repo->getHeaderName('metadataParentProperty')] = $this->metadataParentProperty;
        }
        return $h;
    }

    public function toQuery(): string {
        return http_build_query($this->toArray());
    }
}
