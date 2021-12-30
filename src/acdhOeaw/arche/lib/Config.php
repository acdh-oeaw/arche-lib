<?php

/*
 * The MIT License
 *
 * Copyright 2021 zozlak.
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

use acdhOeaw\arche\lib\exception\RepoLibException;
use function GuzzleHttp\json_decode;
use function GuzzleHttp\json_encode;

/**
 * A container for the yaml configuration allowing to satisfy phpstan checks
 * by mocking config properties hierarchy.
 *
 * @author zozlak
 * @property Config $accessControl
 * @property array  $addNamespaces
 * @property string $address
 * @property string $admin
 * @property array  $adminRoles
 * @property array  $allowedRoles
 * @property array  $assignRoles
 * @property Config $autoAddIds
 * @property Config $auth
 * @property array<Config> $authMethods
 * @property string $binaryModificationDate
 * @property string $binaryModificationUser
 * @property string $binarySize
 * @property string $class
 * @property int    $checkInterval
 * @property array<string, string> $classLoader
 * @property string $connStr
 * @property array  $copying
 * @property string $cors
 * @property Config $create
 * @property string $creationDate
 * @property string $creationUser
 * @property array  $creatorRights
 * @property string $dataCol
 * @property Config $db
 * @property Config $dbConn
 * @property string $dbConnStr
 * @property array<string, array<Config>> $default
 * @property string $defaultAction
 * @property string $defaultMetadataFormat
 * @property string $defaultMetadataReadMode
 * @property string $defaultMetadataSearchMode
 * @property string $defaultMetadataWriteMode
 * @property string $defaultMime
 * @property string $delete
 * @property array  $denyNamespaces
 * @property string $dir
 * @property bool   $enforceCompleteness
 * @property bool   $enforceOnMetadata
 * @property bool   $exceptionOnTimeout
 * @property string $file
 * @property string $fileName
 * @property array<string, array<Config>> $fixed
 * @property array  $forbidden
 * @property Config $fullTextSearch
 * @property string $function
 * @property string $guest
 * @property Config $handlers
 * @property string $hash
 * @property string $hashAlgorithm
 * @property Config $headers
 * @property string $highlighting
 * @property string $host
 * @property Config $httpBasic
 * @property object $httpHeader
 * @property string $id
 * @property string $indexing
 * @property string $label
 * @property string $lang
 * @property string $level
 * @property int    $levels
 * @property Config $logging
 * @property \EasyRdf\Resource $meta
 * @property string $metadata
 * @property array  $metadataFormats
 * @property Config $metadataManager
 * @property Config $metadataManagment
 * @property string $metadataParentProperty
 * @property string $metadataReadMode
 * @property string $metadataWriteMode
 * @property array  $methods
 * @property array  $mime
 * @property Config $mimeFilter
 * @property array  $mimeTypes
 * @property string $modeDir
 * @property string $modificationDate
 * @property string $modificationUser
 * @property array  $namespaces
 * @property array<string> $nonRelationProperties
 * @property array<string> $options
 * @property array  $parameters
 * @property string $parent
 * @property string $path
 * @property string $pathBase
 * @property string $password
 * @property int    $port
 * @property string $publicRole
 * @property array  $properties
 * @property string $queue
 * @property Config | null $rabbitMq
 * @property string $read
 * @property Config $rest
 * @property Config $schema
 * @property string $searchCount
 * @property string $searchFts
 * @property string $searchMatch
 * @property bool   $simplifyMetaHistory
 * @property string $sizeLimit
 * @property Config $sizeLimits
 * @property array  $skipNamespaces
 * @property Config $socket
 * @property Config $spatialSearch
 * @property Config $storage
 * @property string $table
 * @property string $tikaLocation
 * @property int    $timeout
 * @property int    $lockTimeout
 * @property int    $statementTimeout
 * @property string $tmpDir
 * @property Config $transactionController
 * @property string $transactionId
 * @property string $type
 * @property string $uri
 * @property string $urlBase
 * @property string $user;
 * @property string $userCol
 * @property string $value
 * @property bool   $verifyCert
 * @property float  $version
 */
class Config {

    static public function fromYaml(string $path): Config {
        return new self((object) json_decode(json_encode(yaml_parse_file($path))));
    }

    private object $cfg;

    public function __construct(object $config) {
        if ($config instanceof Config) {
            $this->cfg = $config->cfg;
        } else {
            $this->cfg = $config;
        }
    }

    public function __get(string $name): mixed {
        return $this->cfg->$name ?? null;
    }

    public function __set(string $name, mixed $value): void {
        $this->cfg->$name = $value;
    }

    public function __isset(string $name): bool {
        return isset($this->cfg->$name);
    }

    public function asObject(): object {
        return $this->cfg;
    }

    /**
     * 
     * @return array<mixed>
     */
    public function asArray(): array {
        return (array) json_decode(json_encode($this->cfg), true);
    }

    public function asYaml(): string {
        return yaml_emit($this->asArray());
    }
}
