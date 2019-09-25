# Migration from repo-php-lib

## Main class names

* `Fedora` is now `Repo`
* `FedoraResource` is now `RepoResource`


## Initialization

In the repo-php-util configuration was stored in a dedicated singleton class which had to be initialized before the `Fedora` class constructor was called:

```php
use acdhOeaw\util\RepoConfig;
use acdhOeaw\fedora\Fedora;
RepoConfig::init('path/to/config.ini');
$fedora = new Fedora();
```

Now the `Repo` class constructor explicitely takes all the required configuration data (see the API documentation).
It allows to instantiate many `Repo` objects using different configurations which was impossible with the repo-php-util singleton design.

To allow a straightforward `Repo` object creation a static method `Repo::factory()` is provided which calls the `Repo` class constructor 
with a configuration extracted from a given configuration file (which is now a YAML file):

```php
use acdhOeaw\acdhRepoLib\Repo;
$repo = Repo::factory('path\to\config.yaml');
```

## Creating RepoResource objects

In the repo-php-util `FedoraResource` objects where created using `Fedora::getResourceByUri()` method:

```php
use acdhOeaw\util\RepoConfig;
use acdhOeaw\fedora\Fedora;
RepoConfig::init('path/to/config.ini');
$fedora = new Fedora();
$res = $fedora->getResourceByUri('https://resource.url');
```

Now you simply call the `RepoResource` object constructor:

```php
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
$repo = Repo::factory('path/to/config.yaml');
$res = new RepoResource('https://resource.url', $repo);
```

## Fetching metadata

There are two important changes in regard to metadata access:

* new metadata getters and setters;
* new metadata fetch modes.

### RepoResource::getMetadata() vs RepoResource::getGraph() and RepoResource::setMetadata() vs RepoResource::setGraph()

In the repo-php-util `FedoraResource::getMetadata()` and `FedoraResource::setMetadata()` methods always created a deep copy of returned/taken metadata objects, e.g.:
```php
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
$repo = Repo::factory('path/to/config.yaml');
$res = new RepoResource('https://resource.url', $repo);
$meta1 = $res->getMetadata();
$meta1->addLiteral('https://my.property', 'my value');
$meta2 = $res->getMetadata();
echo (int) ($meta1->getGraph->serialise('ntriples') === $meta2->getGraph->serialise('ntriples'));
// displays 0 because $meta1 contains the additional triple, $meta2 does not
```

This approach is safe and protects you from shooting your own foot but it leads to quite a lot data copying.
If you know you will use the metadata read only (or you are aware what you are doing) you can avoid this overhead by returning/passing references to metadata objects.
This is what `RepoResource::getGraph()` and `RepoResource::setGraph()` methods are meant for, e.g.:

```php
use acdhOeaw\acdhRepoLib\RepoResource;
// initialization code skipped
$res = $repo->getResourceByUrl('https://very.large/collection/url');
$res->loadMetadata();
$meta1 = $res->getGraph();
$meta1->addLiteral('https://my.property', 'my value');
$meta2 = $res->getMetadata();
echo (int) ($meta1->getGraph->serialise('ntriples') === $meta2->getGraph->serialise('ntriples'));
// displays 1, also $res->getGraph() is much faster than $res->getMetadata()
```

Another important use case for the `RepoResource::getGraph()` and `RepoResource::setGraph()` is getting/setting 
metadata broader then triples having the resource as a subject, e.g. getting all the data fetched in broad metadata fetch modes (see below)
or setting search results including connected resources metadata.

### RepoResource::getMetadata() $force parameter

The `RepoResource::getMetadata()` doesn't take the `$force` parameter any longer.

Use the `RepoResource::loadMetadata()` method to reload the metadata.

### Metadata fetch modes

The new repository solution offers many metadata fetch modes:

* _resource_ - same as in repo-php-util - only resource metadata are returned;
* _neighbors_ - metadata of the resource and all resources pointed to by the resource metadata are returned
  (convenient e.g. when you want to display a particular resource view);
* _relatives_ - metadata of the resource and all resources pointed recursively (in any direction) by a given metadata property are returned
  (convenient e.g. when you want to display a whole collection tree).

To make it possible to select the fetch mode the `RepoResource::loadMetadata(bool $force, string $mode = RepoResource::META_NEIGBOURS, string $parentProperty = null)` method has been introduced.
Also `Repo::getResourcesBy...()` methods take the `$mode` and `$parentProperty` parameters allowing to specify a desired metadata fetch mode.
You can use `RepoResource::META_RESOURCE`, `RepoResource::META_NEIGBOURS` and ``RepoResource::META_RELATIVES` constants to denote the desired metadata mode, e.g.:

```php
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
$repo = Repo::factory('path/to/config.yaml');
$res = new RepoResource('https://resource.url', $repo);
$res->loadMetadata(true, RepoResource::META_NEIGBOURS);
$meta = $res->getGraph();
$authorName = $meta->getResource('https://author.property')->getLiteral('https://name.property')->getValue();
// it's worth to mention that it won't work with:
$meta = $res->getMetadata();
$authorName = $meta->getResource('https://author.property')->getLiteral('https://name.property')->getValue();
```

If you use the the `RepoResource::getMetadata()` or the `RepoResource::getGraph()` on an object without metadata, 
they will call the `RepoResource::loadMetadata()` with a default parameter values (meaning obtaining metadata in the `RepoResource::META_RESOURCE` mode).

## Searching for resources

On one hand the search API has been simplified to only two methods:

* `Repo::getResourcesBySearchTerms()`
* `Repo::getResourcesBySqlQuery()`

On the other hand many new features have been introduced:

* various metadata fetch modes (see above)
* paging
* full text search highlighting

### Searching by SQL query

The `Repo::getResourcesBySqlQuery(string $query, array $parameters, SearchConfig $config)` 
allows you to execute parameterized SQL queries.

Parameters are denoted in the query with the `?` sign and substituted based on the order.

The SQL query must return an id column with repository resource identifiers matching the search.
Fetching other columns with the query is useless, they will be discarded anyway.

The database structure is as follows:

* resources:
    * id - resource primary identifier
    * transaction_id - id of the transaction having lock on a given resource (null if not locked by any transaction)
    * state - state of a given resource - `active`, `tombstone` or `deleted` 
      (resources are kept in `deleted` state until the end of transaction, then they are removed for sure)
* identifiers - stores identifiers:
    * id - resource primary identifier
    * ids - secondary identifier value
* relations - stores RDF triples pointing to other resources:
    * id - resource primary identifier
    * target_id - target resource identifier
    * property - RDF property of the triple
* metadata - stores all literal RDF triples:
    * mid - row table primary identifier
    * id - resource primary identifier
    * property - RDF property of the triple
    * type - RDF triple value type
    * lang - RDF triple value language
    * value_n - triple value casted to a number (to allow proper comparisons)
    * value_t - triple value casted to a timestamp (to allow proper comparisons)
    * value - triple value as a string
* full_text_search - search full text search indices:
    * ftsid - row table primary identifier
    * id - resource primary identifier
    * property - RDF property of the metadata triple (or `BINARY` for the resource binary content)
    * segments - segmentized and indexed content of the metatadata triple / resource binary content
    * raw - string value of the metatadata triple / resource binary content (required for highlighting)

Advanced search options are controlled by the `SearchConfig` object (see below).

### Searching with SearchTerms

The `Repo::getResourcesBySearchTerms()` method allows to perform search without constructing an SQL query.

Search criteria are described by `SearchTerm` objects. A repository must match all criteria to be included in the search results.

Every `SearchTerm` object can describe any combination of an RDF subject, property, value, type and language as well as an operator used to compare the value, e.g.

* To search for resources having given value of a given triple `new SearchTerm('https://my.property', 'desired value', '=')`
    * To limit to particular language `new SearchTerm('https://my.property', 'desired value', '=', null, 'en')`
* To search for resources having any value of a given triple `new SearchTerm('https://my.property')`
* To search for resources having any triple with a given value `new SearchTerm(null, 'desired value', '=')`
* To search for resources having given triple with a value greater then a give one `new SearchTerm('https://my.property', 'desired value', '>=')`
    * To make sure numbers and dates are compared properly it's better to explicitely provide a type
      `new SearchTerm('https://my.property', 10, '>=', \zozlak\RdfConstants::XSD_DECIMAL)` or
      `new SearchTerm('https://my.property', '2019-01-01', '>=', \zozlak\RdfConstants::XSD_DATE)`
* To perform a regex search on a given property `new SearchTerm('https://my.property', '[a-z]+', '~')`
* To perform a full text search on a given property `new SearchTerm('https://my.property', 'desired value', '@@')`

Advanced search options are controlled by the `SearchConfig` object (see below).

### Full text search

Use the search term search with `@@` as an operator.

### Search config

The `$config` parameter allows to control advanced search configuration:

* paging
* metadata fetch modes
* full text search highlighting

#### Paging

Just set the `offset` and `limit` properties of the `SearchConfig` object.

#### Metadata fetch modes

See the description above.

#### Full text search highlighting

All the parameters beginning with `fts` refer to the full text search results highlighting.

For a detailed description see https://www.postgresql.org/docs/11/textsearch-controls.html#TEXTSEARCH-HEADLINE

The only required property to be set is the `ftsQuery` which is basically the search string used for the full text search hihghlighting.

The `ftsProperty` can be used to limit highlighting results to a particular metadata property.
A special value `BINARY` can be used to indicated resource binary payload.

Fts highlighting result is returned as a special RDF property of the resources metadata (see the example below).

Internally the highlighted results are obtained with the `ts_headline('simple', raw, websearch_to_tsquery('simple', {ftsQuery}), {ftsOptions})`
where `{ftsQuery}` is a corresponding `SearchConfig` object property value and `{ftsOptions}` is a concatenation of `ftsStartSel`, `ftsStopSel`, 
`ftsMaxWords`, `ftsMinWords`, `ftsShortWord`, `ftsHighlightAll`, `ftsMaxFragments` and `ftsFragmentDelimiter` properties.

Remember above-mentioned parameters refer only to the full text search results highlighting which are technically independent from the search filters.
Setting `ftsQuery` doesn't filter for resources matching it. To get this behaviour you should:

* If using `Repo::getResourcesBySearchTerms()` use a `SearchTerm` with `property` equal to `ftsProperty`, `operator` equal to `@@` and `value` equal to `ftsQuery`.
* If using `Repo::getResourcesBySqlQuery()` it should be something like 
  `SELECT id FROM full_text_search WHERE property = {ftsProperty} AND websearch_to_tsquery('simple', {ftsQuery}) @@ segments`.

An example search for all resources containing a given phrase in its binary content and display full text search highlighting results for all matched ones.

```php
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\SearchConfig;
use acdhOeaw\acdhRepoLib\SearchTerm;
$repo = Repo::factory('path/to/config.yaml');
$config = new SearchConfig();
$config->ftsQuery = 'my phrase';
$results = $repo->getResourcesBySearchTerm([new SearchTerm('BINARY', 'my phrase', '@@')], $config);
foreach ($results as $res) {
    echo (string) $res->getGraph()->getLiteral($repo->getSchema()->searchFts) . "\n";
}
```

## Deleting metadata

To make it easy to remove a given RDF property from resources metadata a special syntax has been introduced:

```php
$repo = \acdhOeaw\acdhRepoLib\Repo::factory('config.yaml');
$repo->getResourceById('https://my.id', '\acdhOeaw\acdhRepoAcdh\RepoResource');
$meta = $repo->getGraph();
$meta->addResource($repo->getSchema()->delete, 'https://unwanted.property');
$repo->updateMetadata();
```

## Using extension libraries

The repo-php-util contained everything from the raw Fedora API wrappers up to highly abstract ACDH concepts like dissemination services.

The acdh-repo-lib is different. It provides only a new repository solution API wrappers while ACDH-specific features were moved to separate libraries 
[acdh-repo-acdh](https://github.com/zozlak/acdh-repo-acdh] and [acdh-repo-ingest](https://github.com/zozlak/acdh-repo-ingest).

As in the new solution objects representing repository resources are instantiated directly it's enough to call its constructor to get a specialized object,  e.g.:
```php
$repo = \acdhOeaw\acdhRepoLib\Repo::factory('config.yaml');
$res = new \acdhOeaw\acdhRepoAcdh\RepoResource('https://my.url', $repo);
$res->getDissServices();
```

Things are more complex when it comes to search results. To instantiate search result repository objects with a particular class 
you should use the `$class` property, e.g.:
```php
$repo = \acdhOeaw\acdhRepoLib\Repo::factory('config.yaml');

$repo->getResourceById('https://my.id', '\acdhOeaw\acdhRepoAcdh\RepoResource');

$term = new \acdhOeaw\acdhRepoLib\SearchTerm('https://my.property', 'my value');
$config = new \acdhOeaw\acdhRepoLib\SearchConfig();
$config->class = '\acdhOeaw\acdhRepoAcdh\RepoResource';
$results = $repo->getResourcesBySearchTerms([$term], $config);
```
