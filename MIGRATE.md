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

### Metadata fetch modes

The new repository solution offers many metadata fetch modes:

* _resource_ - same as in repo-php-util - only resource metadata are returned;
* _neigbours_ - metadata of the resource and all resources pointed to by the resource metadata are returned
  (convenient e.g. when you want to display a particular resource view);
* _relatives_ - metadata of the resource and all resources pointed recursively (in any direction) by a given metadata property are returned
  (convenient e.g. when you want to display a whole collection tree).

To make it possible to select the fetch mode the `RepoResource::loadMetadata(bool $force, string $mode = RepoResource::META_NEIGBOURS, string $parentProperty = null)` method has been introduced.
Also `Repo::getResourcesBy...()` methods take the `$mode` and `$parentProperty` parameters allowing to specify a desired metadata fetch mode.
You can use `RepoResource::META_RESOURCE`, `RepoResource::META_NEIGBOURS` and ``RepoResource::META_RELATIVES` constants to denote the desired metadata mode.

Examples:

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